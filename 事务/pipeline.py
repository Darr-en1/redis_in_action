import threading
import time

from setting import redis_conn

redis_conn.delete("notrans:")

"""
redis 事务不支持回滚,因此不具备一致性，原子性
但是redis server 单线程模式可以保证线程安全，因此最终结果一致
"""


def notrans():
    print(redis_conn.incr("notrans:"))
    time.sleep(.3)
    redis_conn.incr('notrans:', -1)


def no_thread_run():
    for i in range(10):
        notrans()
        time.sleep(.1)


def thread_run():
    for i in range(10):
        threading.Thread(target=notrans).start()
        time.sleep(.1)


thread_run()

time.sleep(2)
print("end", redis_conn.get("notrans:"))
