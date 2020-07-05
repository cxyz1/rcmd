from server import cache_client
import logging
from datetime import datetime

logger = logging.getLogger('recommend')


def get_cache_from_redis_hbase(temp,hbu):
    """
    读取用户缓存结果
    :param temp:用户请求参数
    :param hbu:hbase读取工具
    :return:
    """
    # 1.从redis的8号数据库读取
    key = 'reco:{}:{}:art'.format(temp.user_id,temp.channel_id)
    res = cache_client.zrevrange(key,0,temp.article_num - 1)
    # 如果redis有，读取需要的文章数量放回，并删除这些文章
    if res:
        cache_client.zrem(key,*res)

    # 2.如果redis没有，从wait_recommend中读取，放入redis
    else:
        # 删除这个键，否则从wait_recommend中写入redis时会冲突
        cache_client.delete(key)

        try:
            # eval()可以将b'[]'类型转换成[]
            hbase_cache = eval(hbu.get_table_row('wait_recommend',
                                                'reco:{}'.format(temp.user_id).encode(),
                                                 'channel:{}'.format(temp.channel_id).encode()))
        except Exception as e:
            logger.warning("{} WARN read user_id:{} wait_recommend exception:{} not exist".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, e))

            hbase_cache = []

        # 如果wait_recommend也没有，则返回空
        if not hbase_cache:
            return hbase_cache

        # 3.如果wait_recommend有，则取出指定结果存入redis，剩余放回wait_recommend
        if len(hbase_cache) > 100:
            logger.info(
                "{} INFO reduce user_id:{} channel:{} wait_recommend data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

            cache_redis = hbase_cache[:100]
            cache_client.zadd(key,dict(zip(cache_redis,range(len(cache_redis)))))

            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'channel:{}'.format(temp.channel_id).encode(),
                              str(hbase_cache[100:]).encode())
        else:
            # 如果wait_recommend不够一定数量，则全部取出放入redis
            logger.info("{} INFO delete user_id:{} channel:{} wait_recommend data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
            # 清空wait_recommend
            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'channel:{}'.format(temp.channel_id).encode(),
                              str([]).encode())

            cache_client.zadd(key,dict(zip(hbase_cache,range(len(hbase_cache)))))

        res = cache_client.zrevrange(key,0,temp.article_num - 1)

        if res:
            # 从redis拿出结果后，要将redis清空
            cache_client.zrem(key,*res)

    # 4.推荐结果写入历史推荐表
    # reids中读取出来的为str类型，先转成int
    res = list(map(int,res))
    logger.info("{} INFO get cache data and store user_id:{} channel:{} cache history data".format(
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

    hbu.get_table_put('history_recommend1',
                      'reco:his:{}'.format(temp.user_id).encode(),
                      'channel:{}'.format(temp.channel_id).encode(),
                      str(res).encode(),
                      timestamp=temp.time_stamp)

    return res


