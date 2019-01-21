/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.crs4.pydoop.mapreduce.pipes;


import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.conf.Configuration;
/*
  FIXME org.apache.hadoop.mapred.TaskLog is clearly not what it is expected to
  be used with org.apache.hadoop.mapreduce.* 

  For the time being, we use the following as a stand-in.

  it.crs4.pydoop.mapreduce.pipes.TaskLog;
*/

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is responsible for launching and communicating with the child 
 * process.
 */
class Application<K1 extends Writable, V1 extends Writable,
                  K2 extends WritableComparable, V2 extends Writable> {
    private static final Log LOG = LogFactory.getLog(Application.class.getName());
    private ServerSocket serverSocket;
    private Process process;
    private Socket clientSocket;
    private OutputHandler<K2, V2> handler;
    private DownwardProtocol<K1, V1> downlink;
    static final boolean WINDOWS 
        = System.getProperty("os.name").startsWith("Windows");

    /**
     * Start the child process to handle the task for us.
     * @throws IOException
     * @throws InterruptedException
     */
    Application(TaskInputOutputContext<K1,V1,K2,V2> context, 
                DummyRecordReader input) 
        throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        OutputCommitter committer = context.getOutputCommitter();
        if (committer instanceof FileOutputCommitter) {
          conf.set(MRJobConfig.TASK_OUTPUT_DIR,
                   ((FileOutputCommitter)committer).getWorkPath().toString());
        }
        serverSocket = new ServerSocket(0);
        Map<String, String> env = new HashMap<String,String>();
        // add TMPDIR environment variable with the value of java.io.tmpdir
        env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
        env.put(Submitter.PORT, Integer.toString(serverSocket.getLocalPort()));

        // This password is used as a shared secret key between this
        // application and the child pipes process
        byte[] password = null;

        //Add token to the environment if security is enabled
        Token<JobTokenIdentifier> jobToken = 
            TokenCache.getJobToken(context.getCredentials());
        if (null != jobToken) {
          password = jobToken.getPassword();
          String localPasswordFile = new File(".") + Path.SEPARATOR
            + "jobTokenPassword";
          writePasswordToLocalFile(localPasswordFile, password, conf);
          env.put("hadoop.pipes.shared.secret.location", localPasswordFile);
        }
 
        List<String> cmd = new ArrayList<String>();
        String interpretor = conf.get(Submitter.INTERPRETOR);
        if (interpretor != null) {
            cmd.add(interpretor);
        }
        String executable = context.getLocalCacheFiles()[0].toString();
        if (!(new File(executable).canExecute())) {
            // LinuxTaskController sets +x permissions on all distcache files already.
            // In case of DefaultTaskController, set permissions here.
            FileUtil.chmod(executable, "u+x");
        }
        cmd.add(executable);
        // wrap the command in a stdout/stderr capture
        // we are starting map/reduce task of the pipes job. this is not a cleanup
        // attempt. 
        TaskAttemptID taskid = context.getTaskAttemptID();

        File stdout = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDOUT);
        File stderr = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDERR);
        long logLength = TaskLog.getTaskLogLength(conf);
        cmd = TaskLog.captureOutAndError(null, cmd, stdout, stderr, logLength,
                                         false);
        process = runClient(cmd, env);
        clientSocket = serverSocket.accept();
    
        String challenge = null;
        String digestToSend = null;
        String digestExpected = null;
        if (null != password) {
          challenge = getSecurityChallenge();
          digestToSend = createDigest(password, challenge);
          digestExpected = createDigest(password, digestToSend);
        }
        handler = new OutputHandler<K2, V2>(context, input, digestExpected);
        K2 outputKey = (K2)
            ReflectionUtils.newInstance(context.getOutputKeyClass(), conf);
        V2 outputValue = (V2) 
            ReflectionUtils.newInstance(context.getOutputValueClass(), conf);
        downlink = new BinaryProtocol<K1, V1, K2, V2>(clientSocket, handler, 
                                                      outputKey, outputValue, conf);

        if (null != digestToSend) {
          downlink.authenticate(digestToSend, challenge);
          waitForAuthentication();
          LOG.debug("Authentication succeeded");
        }
        downlink.start();
        downlink.setJobConf(conf); 
    }

    private String getSecurityChallenge() {
        Random rand = new Random(System.currentTimeMillis());
        //Use 4 random integers so as to have 16 random bytes.
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(rand.nextInt(0x7fffffff));
        strBuilder.append(rand.nextInt(0x7fffffff));
        strBuilder.append(rand.nextInt(0x7fffffff));
        strBuilder.append(rand.nextInt(0x7fffffff));
        return strBuilder.toString();
    }

    private void writePasswordToLocalFile(String localPasswordFile,
                                          byte[] password, 
                                          Configuration conf) throws IOException {
        FileSystem localFs = FileSystem.getLocal(conf);
        Path localPath = new Path(localPasswordFile);
        FSDataOutputStream out = FileSystem.create(localFs, localPath,
                                                   new FsPermission("400"));
        out.write(password);
        out.close();
    }

    /**
     * Get the downward protocol object that can send commands down to the
     * application.
     * @return the downlink proxy
     */
    DownwardProtocol<K1, V1> getDownlink() {
        return downlink;
    }
  
    /**
     * Wait for authentication response.
     * @throws IOException
     * @throws InterruptedException
     */
    void waitForAuthentication() throws IOException,
        InterruptedException {
        downlink.flush();
        LOG.debug("Waiting for authentication response");
        handler.waitForAuthentication();
    }
  
    /**
     * Wait for the application to finish
     * @return did the application finish correctly?
     * @throws Throwable
     */
    boolean waitForFinish() throws Throwable {
        downlink.flush();
        return handler.waitForFinish();
    }

    /**
     * Abort the application and wait for it to finish.
     * @param t the exception that signalled the problem
     * @throws IOException A wrapper around the exception that was passed in
     */
    void abort(Throwable t) throws IOException {
        LOG.info("Aborting because of " + StringUtils.stringifyException(t));
        try {
            downlink.abort();
            downlink.flush();
        } catch (IOException e) {
            // IGNORE cleanup problems
        }
        try {
            handler.waitForFinish();
        } catch (Throwable ignored) {
            process.destroy();
        }
        IOException wrapper = new IOException("pipe child exception");
        wrapper.initCause(t);
        throw wrapper;      
    }
  
    /**
     * Clean up the child procress and socket.
     * @throws IOException
     */
    void cleanup() throws IOException {
        serverSocket.close();
        try {
            downlink.close();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }      
    }

    /**
     * Run a given command in a subprocess, including threads to copy its stdout
     * and stderr to our stdout and stderr.
     * @param command the command and its arguments
     * @param env the environment to run the process in
     * @return a handle on the process
     * @throws IOException
     */
    static Process runClient(List<String> command, 
                             Map<String, String> env) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        if (env != null) {
            builder.environment().putAll(env);
        }
        Process result = builder.start();
        return result;
    }
  
    public static String createDigest(byte[] password, String data)
        throws IOException {
        SecretKey key = JobTokenSecretManager.createSecretKey(password);
        return SecureShuffleUtils.hashFromString(data, key);
    }

}
