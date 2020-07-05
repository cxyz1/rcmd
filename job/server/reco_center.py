import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
import hashlib
from setting.default import RAParam
from server.utils import HBaseUtils
from server.recall_service import ReadRecall
from server.redis_cache import get_cache_from_redis_hbase
from server.sort_service import lr_sort_service,wdl_sort_service
from server import pool
from datetime import datetime
import logging
import json

logger = logging.getLogger('recommend')

sort_dict = {
    "LR":lr_sort_service,
    "WDL":wdl_sort_service
}

def add_track(res,temp):
    """
    封装埋点参数
    :param res: 推荐文章id列表
    :param temp: 用户行为对象
    :return: track {}
    """
    track = {}

    # 曝光参数
    _exposure = {"action":"exposure","user_id":temp.user_id,"article_id":json.dumps(res),
                 "algorithmCombine":temp.algo}
    track['param'] = json.dumps(_exposure)

    # 每个article_id对应的行为
    track['recommends'] = []
    try:
        for _id in res:
            _dic = {}
            _dic['article_id'] = _id
            _dic['param'] = {}

            # 准备click行为数据
            _p = {'action':'click','user_id':temp.user_id,'article_id':_id,'algorithmCombine':temp.algo}
            _dic['param']['click'] = json.dumps(_p)
            # 准备collect行为数据
            _p['action'] = 'collect'
            _dic['param']['collect'] = json.dumps(_p)
            # 准备share行为数据
            _p['action'] = 'share'
            _dic['param']['share'] = json.dumps(_p)
            # 准备read行为数据
            _p['action'] = 'read'
            _dic['param']['read'] = json.dumps(_p)

            track['recommends'].append(_dic)

    except Exception as e:
        print("推荐列表为空")


    # 构造买点参数的时间戳
    track['timestamp'] = temp.time_stamp
    return track


class RecoCenter(object):
    """推荐中心
    1、处理时间戳逻辑
    2、召回、排序、缓存
    """
    def __init__(self):
        self.hbu = HBaseUtils(pool)
        self.recall_service = ReadRecall()

    def feed_recommend_time_stamp_logic(self, temp):
        """
        用户刷新时间戳的逻辑
        :param temp: ABTest传入的用户请求参数
        :return:
        """
        # 1、获取用户的历史数据库中最近一次时间戳lt
        # 如果过用户没有过历史记录
        try:
            last_stamp = self.hbu.get_table_row('history_recommend1',
                                                'reco:his:{}'.format(temp.user_id).encode(),
                                                'channel:{}'.format(temp.channel_id).encode(),
                                                include_timestamp=True)[1]
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

        except Exception as e:
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp, exception:{}".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id, e))
            last_stamp = 0

        # 2、如果lt < 用户请求时间戳, 用户的刷新操作
        if last_stamp < temp.time_stamp:
            # 走正常的推荐流程
            # 缓存读取、召回排序流程

            # last_stamp应该是temp.time_stamp前面一条数据
            # 返回给用户上一条时间戳给定为last_stamp
            # 2.1获取缓存
            res = get_cache_from_redis_hbase(temp, self.hbu)
            # 如果没有缓存，重新读取召回，排序
            if not res:
                logger.info("{} INFO cache is null get user_id:{} channel:{} recall/sort data".
                            format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

                res = self.user_reco_list(temp)

            # 2.2 直接拿推荐结果，不走缓存
            # res = self.user_reco_list(temp)

            temp.time_stamp = last_stamp
            _track = add_track(res, temp)

            # 读取用户召回结果返回

        else:

            logger.info("{} INFO read user_id:{} channel:{} history recommend data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
            # 3、如果lt >= 用户请求时间戳, 用户才翻历史记录
            # 根据用户传入的时间戳请求，去读取对应的历史记录
            # temp.time_stamp
            # 1559148615353,hbase取出1559148615353小的时间戳的数据， 1559148615354
            try:
                row = self.hbu.get_table_cells('history_recommend1',
                                               'reco:his:{}'.format(temp.user_id).encode(),
                                               'channel:{}'.format(temp.channel_id).encode(),
                                               timestamp=temp.time_stamp + 1,
                                               include_timestamp=True)
            except Exception as e:
                logger.warning("{} WARN read history recommend exception:{}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
                row = []
                res = []

            # [(,), ()]
            # 1559148615353, [15140, 16421, 19494, 14381, 17966]
            # 1558236647437, [18904, 14300, 44412, 18238, 18103, 43986, 44339, 17454, 14899, 18335]
            # 1558236629309, [43997, 14299, 17632, 17120]

            # 3步判断逻辑
            #1、如果没有历史数据，返回时间戳0以及结果空列表
            # 1558236629307
            if not row:
                temp.time_stamp = 0
                res = []
            elif len(row) == 1 and row[0][1] == temp.time_stamp:
                # [([43997, 14299, 17632, 17120], 1558236629309)]
                # 2、如果历史数据只有一条，返回这一条历史数据以及时间戳正好为请求时间戳，修改时间戳为0，表示后面请求以后就没有历史数据了(APP的行为就是翻历史记录停止了)
                res = row[0][0]
                temp.time_stamp = 0
            elif len(row) >= 2:
                res = row[0][0]
                temp.time_stamp = int(row[1][1])
                # 3、如果历史数据多条，返回最近的第一条历史数据，然后返回之后第二条历史数据的时间戳

            # res bytes--->list
            # list str---> int id
            res = list(map(int, eval(res)))

            logger.info(
                "{} INFO history:{}, {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), res, temp.time_stamp))

            _track = add_track(res, temp)
            _track['param'] = ''

        return _track

    def user_reco_list(self, temp):
        """
        用户的下拉刷新获取新数据的逻辑
        - 1、循环算法组合参数，遍历不同召回结果进行过滤
        - 2、过滤当前该请求频道推荐历史结果，如果不是0频道需要过滤0频道推荐结果，防止出现
        - 3、过滤之后，推荐出去指定个数的文章列表，写入历史记录，剩下多的写入待推荐结果
        :return:
        """
        # - 1、循环算法组合参数，遍历不同召回结果进行过滤
        reco_set = []
        # (1, [100, 101, 102, 103, 104], [])
        for number in RAParam.COMBINE[temp.algo][1]:
            if number == 103:
                _res = self.recall_service.read_redis_new_article(temp.channel_id)
                reco_set = list(set(reco_set).union(set(_res)))
            elif number == 104:
                _res = self.recall_service.read_redis_hot_article(temp.channel_id)
                reco_set = list(set(reco_set).union(set(_res)))
            else:
                # 100, 101, 102召回结果读取
                _res = self.recall_service.read_hbase_recall_data(RAParam.RECALL[number][0],
                                                             'recall:user:{}'.format(temp.user_id).encode(),
                                                             '{}:{}'.format(RAParam.RECALL[number][1],
                                                                            temp.channel_id).encode())
                reco_set = list(set(reco_set).union(set(_res)))

        # - 2、过滤当前该请求频道推荐历史结果，如果不是0频道需要过滤0频道推荐结果，防止出现其他频道和0频道重复
        history_list = []
        try:
            data = self.hbu.get_table_cells('history_recommend1',
                                            'reco:his:{}'.format(temp.user_id).encode(),
                                            'channel:{}'.format(temp.channel_id).encode())

            for _ in data:
                history_list = list(set(history_list).union(set(eval(_))))

            logger.info("{} INFO read user_id:{} channel_id:{} history data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
        except Exception as e:
            # 打印日志
            logger.warning(
                "{} WARN filter history article exception:{}".format(datetime.now().
                                                                     strftime('%Y-%m-%d %H:%M:%S'), e))

        try:
            data = self.hbu.get_table_cells('history_recommend1',
                                            'reco:his:{}'.format(temp.user_id).encode(),
                                            'channel:{}'.format(0).encode())

            for _ in data:
                history_list = list(set(history_list).union(set(eval(_))))

            logger.info("{} INFO read user_id:{} channel_id:{} history data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, 0))

        except Exception as e:
            # 打印日志
            logger.warning(
                "{} WARN filter history article exception:{}".format(datetime.now().
                                                                     strftime('%Y-%m-%d %H:%M:%S'), e))

        # reco_set  history_list
        # - 3、过滤之后，推荐出去指定个数的文章列表，写入历史记录，剩下多的写入待推荐结果
        reco_set = list(set(reco_set).difference(set(history_list)))
        print("召回数据：", reco_set)
        logger.info(
            "{} INFO after filter history:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), reco_set))

        # 如果过滤完历史数据后为空，直接返回，不用走排序
        if not reco_set:

            return reco_set
        else:
            # 模型对推荐的结果排序
            # temp.user_id， reco_set
            _sort_num = RAParam.COMBINE[temp.algo][2][0]
            # 'LR'
            reco_set = sort_dict[RAParam.SORT[_sort_num]](reco_set, temp, self.hbu)
            print("排序数据：", reco_set)



            # 如果reco_set小于用户需要推荐的文章
            if len(reco_set) <= temp.article_num:
                res = reco_set
            else:
                # 大于要推荐的文章结果
                res = reco_set[:temp.article_num]

                # 将剩下的文章列表写入待推荐的结果
                self.hbu.get_table_put('wait_recommend',
                                       'reco:{}'.format(temp.user_id).encode(),
                                       'channel:{}'.format(temp.channel_id).encode(),
                                       str(reco_set[temp.article_num:]).encode(),
                                       timestamp=temp.time_stamp)

                # res在外面写入历史记录

            # 直接写入历史记录当中，表示这次又成功推荐一次
            self.hbu.get_table_put('history_recommend1',
                                   'reco:his:{}'.format(temp.user_id).encode(),
                                   'channel:{}'.format(temp.channel_id).encode(),
                                   str(res).encode(),
                                   timestamp=temp.time_stamp)

            return res

