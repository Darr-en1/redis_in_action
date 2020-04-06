import bisect
import time

from redis import WatchError
from redis.client import Pipeline

from setting import redis_conn

PRECISION = (1, 5, 60, 5 * 60, 60 * 60, 5 * 60 * 60, 24 * 60 * 60,)

QUIT = False

SAMPLE_COUNT = 100  # 搭配精度，用于计算时间


def update_counter(conn: redis_conn, name, count=1, now=None):
    """
    :param conn:    redis连接对象
    :param name:    统计名(点击量，销量...)
    :param count:   访问数
    :param now:   当前时间
    :return:
    """

    now = now or time.time()
    pipe: Pipeline = conn.pipeline()
    for prec in PRECISION:
        # 取得当前时间片的开始时间
        pnow = now // prec * prec
        hash = f'{prec}:{name}'
        pipe.zadd('known:', {hash: 0})
        pipe.hincrby('count:' + hash, pnow, count)
    pipe.execute()


def get_counter(conn, name, precision, data_format="%Y-%m-%d %H:%M:%S"):
    hash = f'{precision}:{name}'
    all_counter = conn.hgetall('count:' + hash)
    return sorted(
        map(lambda obj: (time.strftime(data_format, time.localtime(int(obj[0]))), int(obj[1])), all_counter.items()),
        key=lambda obj: (obj[0], obj[1],))


def clean_counters(conn):
    pipe = conn.pipeline(True)
    # 程序清理操作执行的次数,每执行一次加一
    passes = 0
    # 持续地对计数器进行清理，直到退出为止。
    while not QUIT:
        # 记录清理操作开始执行的时间，用于计算清理操作执行的时长。
        start = time.time()
        # 作为遍历 known(zset)表 value 的 index
        index = 0
        while index < conn.zcard('known:'):
            # 取特定精度的计数器表 key (hash)
            hash = conn.zrange('known:', index, index)
            index += 1
            if not hash:
                break
            hash = hash[0]
            # 取得计数器的精度。
            prec = int(hash.partition(':')[0])
            # 通过精度计算清理的频率(精度为 1 min 以内的设置每次轮询都会清理，大于1min 设置 int(prec // 60)次进行清理)
            bprec = int(prec // 60) or 1
            # 判断当前精度处于当前轮询次数下是否需要被清理
            if passes % bprec:
                continue

            hkey = 'count:' + hash
            # 计算该  间
            cutoff = time.time() - SAMPLE_COUNT * prec
            # 获取该精度下所有样本并排序
            samples = sorted(map(int, conn.hkeys(hkey)))
            # 通过二分查找已经排序的列表中过期的key
            remove = bisect.bisect_right(samples, cutoff)

            if remove:
                # 移除过期计数样本
                conn.hdel(hkey, *samples[:remove])
                # 判断计数样本是否全部过期，过期删除在zset中的内容
                if remove == len(samples):
                    try:
                        # 在尝试修改计数器散列之前，对其进行监视。确保删除时有添加可以拒绝删除操作
                        pipe.watch(hkey)
                        # 验证计数器散列是否为空，如果是的话，那么从记录已知计数器的有序集合里面移除它
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem('known:', hash)
                            pipe.execute()
                            # 删除一个计数器，zset的zcard减一,因此index不变即可获取下一个精度
                            index -= 1
                        else:
                            # 计数器散列并不为空，继续让它留在记录已有计数器的有序集合里面
                            pipe.unwatch()
                    # 删除过程中有其他程序向这个计算器散列添加了新的数据，继续让它留在记录已知计数器的有序集合里面
                    except WatchError:
                        pass

        # 清理次数加一
        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        # 如果这次循环未耗尽60秒钟，那么在余下的时间内进行休眠；
        # 如果60秒钟已经耗尽，那么休眠一秒钟以便稍作休息。
        time.sleep(max(60 - duration, 1))


if __name__ == '__main__':
    for i in range(10):
        update_counter(redis_conn, "hits")

    for prec in PRECISION:
        print(prec, get_counter(redis_conn, "hits", prec))

    clean_counters(redis_conn)
