"""
PUBLISH和SUBSCRIBE的缺陷在于客户端必须一直在线才能接收到消息，断线可能会导致客户端丢失消息。
除此之外，旧版的redis可能会由于订阅者消费不够快(消息堆积在内存)而变的不稳定导致崩溃，甚至被管理员杀掉

第一个原因是和redis系统的稳定性有关。对于旧版的redis来说，如果一个客户端订阅了某个或者某些频道，
但是它读取消息的速度不够快，那么不断的积压的消息就会使得redis输出缓冲区的体积越来越大，
这可能会导致redis的速度变慢，甚至直接崩溃。也可能会导致redis被操作系统强制杀死，
甚至导致操作系统本身不可用。新版的redis不会出现这种问题，
因为它会自动断开不符合client-output-buffer-limit pubsub配置选项要求的订阅客户端

第二个原因是和数据传输的可靠性有关。任何网络系统在执行操作时都可能会遇到断网的情况。
而断线产生的连接错误通常会使得网络连接两端中的一端进行重新连接。如果客户端在执行订阅操作的过程中断线，
那么客户端将会丢失在断线期间的消息，这在很多业务场景下是不可忍受的。
"""

import time

from setting import redis_conn


def subscriber():
    print('subscriber start')
    pubsub = redis_conn.pubsub()
    pubsub.subscribe('channel1', 'sss')
    for item in pubsub.listen():
        print(item)
        if item['data'] == '5':
            # pubsub.unsubscribe()
            pubsub.unsubscribe('channel1')
    print('ssssssssss')
    # while True:
    #     print(pubsub.parse_response())
    #     time.sleep(3)


def psubscriber():
    print('subscriber start')
    pubsub = redis_conn.pubsub()
    pubsub.psubscribe('channel*')
    # for item in pubsub.listen():
    #     print(item)
    while True:
        print(pubsub.parse_response())
        time.sleep(3)


subscriber()
