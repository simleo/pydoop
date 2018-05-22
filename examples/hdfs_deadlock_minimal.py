from multiprocessing import Process
from pydoop.hdfs.core import init
hdfs = init()


def connect():
    fs = None
    try:
        fs = hdfs.CoreHdfsFs("default", 0)
    finally:
        if fs:
            fs.close()


def deadlock():
    connect()
    p = Process(target=connect)
    p.start()
    p.join()


def no_deadlock():
    procs = [Process(target=connect) for _ in range(3)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


if __name__ == "__main__":
    no_deadlock()
