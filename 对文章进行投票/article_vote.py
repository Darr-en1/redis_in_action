"""
对文章进行投票

需求：
对于优质文章的定义： 文章获得至少200张支持票
优质文章会放在文章列表前列（至少一天）
文章投票暂时不提供反对票
文章评分规则： 文章的评分会随时间流逝而减少，并会随着支持票的增加而增加
每个用户对单篇文章只能投票一次
文章发布一周后就不能投票
每一票的分数为432 （24 * 60 * 60 / 200）

分数计算 1970.1.1 ~now的秒数 + 票分（n * 432）
"""
import time

import redis

ONE_WEEK_IN_SECONDS = 7 * 24 * 60 * 60
VOTE_SCORE = 432
SEP = ':'  # 分隔符
TIME_NAME = f'time{SEP}'  # zset_name 文章发布时间列表
SCORE_NAME = f'score{SEP}'  # zset_name 文章评分列表
VOTED_NAME = f'voted{SEP}'  # set_name 单个文章投票用户列表
ARTICLE_NAME = f'article{SEP}'  # hash_name 单个文章具体内容
GROUP_NAME = f'group{SEP}'  # set_name 组别
ARTICLES_PER_PAGE = 25


def highlight(text):
    return f'\033[1;32m {text} \033[0m'


def article_vote(conn: redis.client.Redis, user: str, article: str) -> None:
    """
    用户投票
    """
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    a = conn.zscore(TIME_NAME, article)
    if conn.zscore(TIME_NAME, article) < cutoff:
        return
    article_id = article.partition(SEP)[:-1]  # diff between split and partition
    if conn.sadd(f'{VOTED_NAME}{article_id}', user):
        # before
        # zincrby(self, name, value, amount)
        # now
        # zincrby(self, name, amount, value)
        conn.zincrby(SCORE_NAME, VOTE_SCORE, article)  # zset score increase
        conn.hincrby(article, 'votes', 1)  # hash value increase


def post_article(conn: redis.client.Redis, user: str, title: str, link: str) -> str:
    """
    添加文章
    """
    article_id = str(conn.incr(ARTICLE_NAME))
    voted = f'{VOTED_NAME}{article_id}'
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)  # 设置过去时间
    article = f'{ARTICLE_NAME}{article_id}'
    now = time.time()
    conn.hmset(article, dict(
        title=title,
        link=link,
        poster=user,
        time=now,
        votes=1
    ))
    # before
    # zadd(name,key1,value1,key2,value2)
    # now
    # zadd(name,{key1:value1,key2:value2})
    conn.zadd(SCORE_NAME, {article: now + VOTE_SCORE})
    conn.zadd(TIME_NAME, {article: now})
    return article_id


def get_articles(conn: redis.client.Redis, page: int, order=SCORE_NAME) -> list:
    """
    获取文章（排序）
    """
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1
    ids = conn.zrevrange(order, start, end)
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    return articles


def add_remove_groups(conn: redis.client.Redis, article: str, to_add=[], to_remove=[]) -> None:
    """
    组别设置
    """
    for group in to_add:
        conn.sadd(GROUP_NAME + group, article)
    for group in to_remove:
        conn.srem(GROUP_NAME + group, article)


def get_group_articles(conn: redis.client.Redis, group: str, page: int, order=SCORE_NAME) -> list:
    """
    获取组内文章（排序）
    """
    key = order + group
    if not conn.exists(key):
        conn.zinterstore(key, [GROUP_NAME + group, order], aggregate='max', )
    conn.expire(key, 60)
    return get_articles(conn, page, key)


def show_key(conn: redis.client.Redis, key: str, count=100):
    return list(conn.scan_iter(key, count))


def main():
    def show_articles():
        print('========= new article =========\n' +
              f'{get_articles(conn, 0, TIME_NAME)}\n' +
              '\n' +
              '========= hot article =========\n' +
              f'{get_articles(conn, 0)}\n'
              )

    def add_article():
        title, _, link = input(
            f"Please enter {highlight('title content')} to add article, format:" +
            f"       {highlight('title@content')}\n"
        ).partition('@')
        post_article(conn, username, title, link)

    def vote():
        print(show_key(conn, "article:*"))
        article = input(
            f"Please enter {highlight('article')} to vote, format:" +
            f"       {highlight('article_name')}\n"
        )
        article_vote(conn, username, article)

    def get_article_from_group():
        print(f"group: {show_key(conn, 'group:*')}")

        group, page, order = input(
            f"Please enter {highlight('group page order_choices(hot or new)')} to get article from group, format:" +
            f"       {highlight('group_name@page@hot')}\n"
        ).split('@', 2)
        print(get_group_articles(conn, group, int(page), choices_article_screen.get(order, SCORE_NAME)))

    def add_or_remove_article_in_group():
        print(f"group: {show_key(conn, 'group:*')}")

        article, to_add, to_remove = input(
            f"Please enter {highlight('article to_add to_remove')} to add or remove article in group,"
            f"you can write _ as null for to_add or to_remove,format:" +
            f"       {highlight('article_name@group_name1#group_name2@group_name3#group_name4')}\n"
        ).split('@', 2)
        to_add = [] if to_add == "_" else to_add.split('#')
        to_remove = [] if to_remove == "_" else to_remove.split('#')
        add_remove_groups(conn, article, to_add, to_remove)

    def default():
        print("see you")
        return True

    choices_select = {
        '1': show_articles,
        '2': add_article,
        '3': vote,
        '4': get_article_from_group,
        '5': add_or_remove_article_in_group
    }
    choices_article_screen = {
        'hot': SCORE_NAME,
        'new': TIME_NAME
    }

    host = input('Please enter what you want to connect to redis server: ').strip()
    if not host: host = '192.168.98.128'
    conn = redis.Redis(host=host, decode_responses=True)
    username = input('Enter the name: ').strip()
    if not username: username = 'anonymous_user'

    while True:
        option = input(f"select({highlight('other values exit')}): \n" +
                       '   1:show article\n' +
                       '   2:add article\n' +
                       '   3:vote\n' +
                       '   4:get article from group\n' +
                       '   5:add or remove article in group\n'
                       )

        if choices_select.get(option, default)(): break


if __name__ == '__main__':
    main()
