from server import SORT_SPARK
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.linalg import DenseVector
import numpy as np
import pandas as pd
from datetime import datetime
import logging

import tensorflow as tf
from grpc.beta import implementations
from tensorflow_serving.apis import prediction_service_pb2_grpc
from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import classification_pb2
import os
import sys
import grpc
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

logger = logging.getLogger("recommend")

def lr_sort_service(reco_set,temp,hbu):
    """
    排序返回推荐文章
    :param reco_set:召回并过滤后的结果
    :param temp: 前端传过来的参数
    :param hbu: hbase工具
    :return:
    """
    # 1.读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
    except Exception as e:
        user_feature = []

    if user_feature:
        # 2.读取文章特征中心特征
        result = []

        for article_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:
                article_feature = [0.0] * 111

            f = []
            # 顺序一定要和之前训练模型时的特征顺序一致
            # 第一个channel_id
            f.extend([article_feature[0]])
            # 第二个文章向量
            f.extend(article_feature[11:])
            # 第三个用户特征权重
            f.extend(user_feature)
            # 第四个文章权重
            f.extend(article_feature[1:11])
            vector = DenseVector(f)
            result.append([temp.user_id,article_id,vector])

        # 4.预测并排序
        df = pd.DataFrame(result,columns=["user_id","article_id","features"])
        test = SORT_SPARK.createDataFrame(df)

        # 加载逻辑回归模型
        model = LogisticRegressionModel.load("/headlines/models/logistic_ctr_model.obj")
        predict = model.transform(test)

        # 获取文章点击率
        def vector_to_double(row):
            return float(row.article_id),float(row.probability[1])

        res = predict.select(["article_id","probability"]).rdd.map(vector_to_double).toDF(
            ["article_id","probability"]).sort("probability",ascending=False)
        article_list = [i.article_id for i in res.collect()]
        logger.info("{} INFO sorting user_id:{} recommend article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          temp.user_id))
        # 5.将排名前100个文章id返回给用户推荐
        if len(article_list) > 100:
            article_list = article_list[:100]
        reco_set = list(map(int,article_list))

    return reco_set




def wdl_sort_service(reco_set,temp,hbu):
    """
    wide&deep进行排序预测
    :return:
    """

    # 1.读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

    except Exception as e:
        user_feature = []


    if user_feature:

        # 2.读取文章特征中心特征
        examples = []
        for article_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:
                article_feature = [0.0] * 111

            # 3.serving服务端每一篇文章的example样本构造,要按照训练样本时的顺序和名字
            channel_id = int(article_feature[0])
            vector = np.mean(article_feature[11:])
            user_weights = np.mean(user_feature)
            article_weights = np.mean(article_feature[1:11])

            # 组建example
            example = tf.train.Example(features = tf.train.Features(feature={
                'channel_id':tf.train.Feature(int64_list=tf.train.Int64List(value=[channel_id])),
                'vector':tf.train.Feature(float_list=tf.train.FloatList(value=[vector])),
                'user_weights':tf.train.Feature(float_list=tf.train.FloatList(value=[user_weights])),
                'article_weights':tf.train.Feature(float_list=tf.train.FloatList(value=[article_weights]))
            }))

            examples.append(example)


        # 模型服务调用
        with grpc.insecure_channel("0.0.0.0:8500") as channel:
            stub = prediction_service_pb2_grpc.PredictionServiceStub(channel)

            # 用准备好的example封装成request请求
            # 并指定要调用的模型名称
            request = classification_pb2.ClassificationRequest()
            request.model_spec.name = 'wdl'
            request.input.example_list.examples.extend(examples)

            # 获取结果
            response = stub.Classify(request, 10.0)

            # 将结果与article_id组合，并按点击率大小排序，推荐出去
            num = len(reco_set)
            l = []

            for i in range(num):

                label_1 = max(response.result.classifications[i].classes,key=lambda c:c.label)

                l.append(label_1.score)

            res = sorted(list(zip(reco_set,l)),key=lambda c:c[1],reverse=True)

            article_list = [index[0] for index in res]

            # 将排名前100个文章id返回给用户推荐
            if len(article_list) > 100:
                article_list = article_list[:100]

            reco_set = list(map(int,article_list))

        return reco_set




# if __name__ == '__main__':
#
#     wdl_sort_service()










