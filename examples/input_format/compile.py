#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, subprocess, glob
import pydoop

HADOOP_HOME = pydoop.hadoop_home()
HADOOP_VERSION = pydoop.hadoop_version()
SRC = "net/sourceforge/pydoop/mapred/TextInputFormat.java"
CLASS = SRC.replace("java", "class")
OUTPUT_JAR = "pydoop-mapred.jar"


def string_version(tuple_version):
  return ".".join(map(str, HADOOP_VERSION))

def main(argv):
  hadoop_jars = glob.glob(os.path.join(HADOOP_HOME, "hadoop-*.jar"))
  classpath = ":".join(hadoop_jars)
  subprocess.check_call("javac -cp %s %s" % (classpath, SRC), shell=True) 
  subprocess.check_call("jar -cvf %s %s" % (OUTPUT_JAR, CLASS), shell=True)


if __name__ == "__main__":
  main(sys.argv)