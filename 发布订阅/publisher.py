import time

from setting import redis_conn


def publisher():
    # print(redis_conn.set("1","2"))
    # print(redis_conn.get("1"))
    print("publisher start")
    for i in range(10):
        time.sleep(1)
        print(redis_conn.publish('channel1', i))


publisher()
