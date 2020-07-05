import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from server import redis_client
from server import pool
import logging
from datetime import datetime
from server.utils import HBaseUtils

logger = logging.getLogger('recommend')


class ReadRecall(object):
    """
    读取召回集的结果
    """
    def __init__(self):
        self.client = redis_client
        self.hbu = HBaseUtils(pool)
        self.hot_num = 10

    def read_hbase_recall_data(self,table_name,key_format,column_format):
        """
        读取多路召回als,content,online
        :param table_name:
        :param key_format:
        :param column_format:
        :return:
        """

        recall_list = []
        try:
            data = self.hbu.get_table_cells(table_name,key_format,column_format)
            for _ in data:
                recall_list = list(set(recall_list).union(set(eval(_))))

            # self.hbu.get_table_delete(table_name,key_format,column_format)

        except Exception as e:
            logger.warning("{} WARN read {} recall exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                        table_name, e))
        return recall_list


    def read_redis_new_article(self, channel_id):
        """
        读取用户的新文章
        :param channel_id:
        :return:
        """
        logger.warning("{} INFO read channel {} redis new article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          channel_id))
        key = 'ch:{}:new'.format(channel_id)
        try:

            reco_list = self.client.zrevrange(key, 0, -1)

        except Exception as e:
            logger.warning("{} WARN read new article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))

            reco_list = []

        return list(map(int, reco_list))


    def read_redis_hot_article(self, channel_id):
        """
        读取热门文章召回结果
        :param channel_id: 提供频道
        :return:
        """
        logger.warning("{} INFO read channel {} redis hot article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          channel_id))
        _key = "ch:{}:hot".format(channel_id)
        try:
            res = self.client.zrevrange(_key, 0, -1)

        except Exception as e:
            logger.warning(
                "{} WARN read new article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []

        # 由于每个频道的热门文章有很多，因为保留文章点击次数
        res = list(map(int, res))
        if len(res) > self.hot_num:
            res = res[:self.hot_num]
        return res


    def read_hbase_article_similar(self,table_name,key_format,article_num):
        """
        相似文章召回
        :param table_name:
        :param key_format:
        :param article_num:
        :return:
        """
        try:
            _dic = self.hbu.get_table_row(table_name,key_format)

            res = []
            _srt = sorted(_dic.items(),key=lambda obj:obj[1],reverse=True)
            if len(_srt) > article_num:
                _srt = _srt[:article_num]

            for _ in _srt:
                res.append(int(_[0].decode().split(':')[1]))

        except Exception as e:
            logger.error(
                "{} ERROR read similar article exception: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []
        return res


# if __name__ == '__main__':
#     rr = ReadRecall()
#     # 召回结果的读取封装
#     print(rr.read_hbase_recall_data('cb_recall1', b'recall:user:1', b'als:17'))
#     print(rr.read_redis_new_article(18))
#     print(rr.read_redis_hot_article(18))
#     print(rr.read_hbase_article_similar('article_similar', b'1', 20))
