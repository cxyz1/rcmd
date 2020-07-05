import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from concurrent import futures
from abtest import user_reco_pb2
from abtest import user_reco_pb2_grpc
from server.reco_center import RecoCenter,add_track
from setting.default import DefaultConfig,RAParam
import grpc
import time
import json
import hashlib



def feed_recommed(user_id,channel_id,article_num,time_stamp):
    """
    1、根据web提供的参数，进行分流
    2、找到对应的算法组合之后，去推荐中心调用不同的召回和排序服务
    3、进行埋点参数封装
    :param user_id:
    :param channel_id:
    :param article_num:
    :param time_stamp:
    :return: _track埋点参数结果
    """
    class TempParam(object):
        # 初始化类属性--做测试，随机定几个参数
        user_id = 10
        channel = 10
        article_num = 10
        time_stamp = 10
        algo = ""

    temp = TempParam()
    temp.user_id = user_id
    temp.channel_id = channel_id
    temp.article_num = article_num
    temp.time_stamp = time_stamp

    # 进行用户的分流，用hashlib的md5值将用户id转换成16个（字母加数字），截取其第一个位，看其位于哪个桶，参考default.py
    # ID为空
    if temp.user_id == "":
        return add_track([],temp)

    # ID正常
    code = hashlib.md5(temp.user_id.encode()).hexdigest()[:1]
    if code in RAParam.BYPASS[0]["Bucket"]:
        temp.algo = RAParam.BYPASS[0]["Strategy"]
    else:
        temp.algo = RAParam.BYPASS[1]["Strategy"]
    # _track = add_track([1,2,3,4,5],temp)
    _track = RecoCenter().feed_recommend_time_stamp_logic(temp)

    return _track


class UserRecommendServicer(user_reco_pb2_grpc.UserRecommendServicer):
    """
    推荐接口服务端逻辑写
    """
    def user_recommend(self, request, context):

        # 1.接收请求参数并解析
        user_id = request.user_id
        channel_id = request.channel_id
        article_num = request.article_num
        time_stamp = request.time_stamp

        # 2.获取用户abtest分流，使用用户分流到的算法到推荐中心去获取推荐结果
        #   再将推荐结果封装成埋点参数（响应体）
        _track = feed_recommed(user_id,channel_id,article_num,time_stamp)

        # 将推荐结果封装成响应体(埋点参数)
        # class Temp(object):
        #     user_id = 10
        #     algo = 'test'
        #     time_stamp = 10
        #
        # tp = Temp()
        # tp.user_id = user_id
        # tp.time_stamp = time_stamp
        # _track = add_track([1,2,3,4,5], tp)

        # 3.将响应体封装成grpc消息体，返回给客户端
        # 参数必须和uer_reco.proto文件中定义的一致
        _reco = []
        for d in _track["recommends"]:
            _param2 = user_reco_pb2.param2(click=d['param']['click'],
                                           collect=d['param']['collect'],
                                           share=d['param']['share'],
                                           read=d['param']['read'])
            _param1 = user_reco_pb2.param1(article_id=d['article_id'],params=_param2)
            _reco.append(_param1)

        return user_reco_pb2.Track(exposure=_track['param'], recommends=_reco, time_stamp=_track['timestamp'])

def serve():
    """
    开启grpc的服务
    :return:
    """
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 注册本地服务
    user_reco_pb2_grpc.add_UserRecommendServicer_to_server(UserRecommendServicer(), server)
    #监听端口
    server.add_insecure_port(DefaultConfig.RPC_SERVER)

    # 开始接受请求进行服务
    server.start()
    one_day_in_second = 60 * 60 * 24
    try:
        while True:
            time.sleep(one_day_in_second)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()




