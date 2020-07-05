import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from offline import SparkSessionBase
from datetime import datetime
from datetime import timedelta
import pyspark.sql.functions as F
from setting.default import CHANNEL_INFO
import pyspark
import gc
import logging
from offline.full_cal.article_profile import segmentation,textrank


logger = logging.getLogger('offline')

class UpdateArticle(SparkSessionBase):
    """
    更新文章画像
    """
    SPARK_APP_NAME = "updateArticle"
    ENABLE_HIVE_SUPPORT = True
    SPARK_EXECUTOR_MEMORY = "4g"

    def __init__(self):
        self.spark = self._create_spark_session()

        self.cv_path = "hdfs://hadoop1:9000/headlines/models/cv.model"
        self.idf_path = "hdfs://hadoop1:9000/headlines/models/idf.model"

    def get_cv_model(self):
        from pyspark.ml.feature import CountVectorizerModel
        cv_model = CountVectorizerModel.load(self.cv_path)
        return cv_model

    def get_idf_model(self):
        from pyspark.ml.feature import IDFModel
        idf_model = IDFModel.load(self.idf_path)
        return idf_model

    @staticmethod
    def compute_keywords_tfidf_topk(words_df,cv_model,idf_model):
        """
        保存tfidf值大的前20个
        :param words_df: 分好词的结果
        :param cv_model:
        :param idf_model:
        :return:
        """
        cv_result = cv_model.transform(words_df)
        tfidf_result = idf_model.transform(cv_result)

        def func(partition):
            TOPK = 20
            for row in partition:
                _ = list(zip(row.idfFeatures.indices,row.idfFeatures.values))
                _ = sorted(_,key=lambda x:x[1],reverse=True)
                result = _[:TOPK]

                for words_index,tfidf in result:
                    yield row.article_id,row.channel_id,int(words_index),round(float(tfidf),4)

        _keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(["article_id","channel_id","index","tfidf"])
        return _keywordsByTFIDF

    def merge_article_data(self):
        """
        合并业务中增量更新的文章数据
        :return:
        """
        # 获取文章相关数据, 指定过去一个小时整点到整点的更新数据
        # 如：26日：1：00~2：00，2：00~3：00，左闭右开
        self.spark.sql("use toutiao")
        _yester = datetime.today().replace(minute=0, second=0, microsecond=0)
        start = datetime.strftime(_yester + timedelta(days=0, hours=-1, minutes=0), "%Y-%m-%d %H:%M:%S")
        end = datetime.strftime(_yester, "%Y-%m-%d %H:%M:%S")

        # 合并后保留：article_id、channel_id、channel_name、title、content、sentence
        basic_content = self.spark.sql(
            "select a.article_id, a.channel_id, a.title, b.content from news_article_basic a "
            "inner join news_article_content b on a.article_id=b.article_id where a.review_time >= '{}' "
            "and a.review_time < '{}' and a.status = 2".format(start, end))
        # 增加channel的名字，后面会使用
        basic_content.registerTempTable("temparticle")
        channel_basic_content = self.spark.sql(
            "select t.*, n.channel_name from temparticle t left join news_channel n on t.channel_id=n.channel_id")

        # 利用concat_ws方法，将多列数据合并为一个长文本内容（频道，标题以及内容合并）
        self.spark.sql("use article")
        sentence_df = channel_basic_content.select("article_id", "channel_id", "channel_name", "title", "content", \
                                                   F.concat_ws(
                                                       ",",
                                                       channel_basic_content.channel_name,
                                                       channel_basic_content.title,
                                                       channel_basic_content.content
                                                   ).alias("sentence")
                                                   )
        del basic_content
        del channel_basic_content
        gc.collect()    # gc是python中垃圾回收模块，防止内存泄漏

        logger.info("INFO: merge data complete")

        sentence_df.write.insertInto("article_data")
        return sentence_df

    def generate_article_label(self,sentence_df):
        """
        生成文章关键词的tfidf与textrank值
        :param sentence_df:合并处理好的文章内容
        :return:
        """
        # 对文章内容分词
        words_df = sentence_df.rdd.mapPartitions(segmentation).toDF(["article_id","channel_id","words"])
        cv_model = self.get_cv_model()
        idf_model = self.get_idf_model()

        # 将index和keywords关系表与index和tfidf关系表合并得到keywords和tfidf关系表
        _keywordsByTFIDF = UpdateArticle.compute_keywords_tfidf_topk(words_df,cv_model,idf_model)

        keywordsIndex = self.spark.sql("select keywords,index idx from idf_keywords_values")

        keywordsByTFIDF = _keywordsByTFIDF.join(keywordsIndex,keywordsIndex.idx == _keywordsByTFIDF.index)\
                        .select(["article_id","channel_id","keyword","tfidf"])

        keywordsByTFIDF.write.insertInto("tfidf_keywords_values")

        del cv_model
        del idf_model
        del words_df
        del _keywordsByTFIDF
        gc.collect()

        # 计算textrank
        textrank_keywords_df = sentence_df.rdd.mapPartitions(textrank).toDF\
                            (["article_id","channel_id","keyword","textrank"])
        textrank_keywords_df.write.inserInto("textrank_keywords_values")

        logger.info("INFO:compute tfidf textrank complete")

        return textrank_keywords_df,keywordsIndex

    def get_article_profile(self,textrank_keywords_df,keywordsIndex):
        """
        文章画像构建，关键词：权重 + 主题词
        :param textrank_keywords_df:
        :param keywordsIndex:
        :return:
        """
        keywordsIndex = keywordsIndex.withColumnRenamed("keyword","keyword1")
        result = textrank_keywords_df.join(keywordsIndex,textrank_keywords_df.keyword == keywordsIndex.keyword1)

        # 1.关键词的权重用idf * textrank值表示
        _articleKeywordsWeights = result.withColumn("weight",result.textrank * result.idf).select\
                                (["article_id","channel_id","keyword","weights"])

        # 将关键词权重组合成字典
        _articleKeywordsWeights.registerTempTable("temptable")
        articleKeywordsWeights = self.spark.sql("select article_id,min(channel_id) channel_id,\
                                collect_list(keyword) keyword_list,collect_list(weights) \
                                weights_list from temptable group by article_id")

        def _func(row):
            return row.article_id,row.channel_id,dict(zip(row.keyword_list,row.weights_list))

        articleKeywords = articleKeywordsWeights.rdd.map(_func).toDF(["article_id","channel_id","keyword"])

        # 2.主题词用ifidf和textrank共现的词表示
        topic_sql = "select t.article_id article_id2,collect_set(t.keyword) topics from tfidf_keywords_values t\
                    inner join textrank_keywords_values r where t.keyword == r.keyword group by article_id2"
        articleTopics = self.spark.sql(topic_sql)

        # 3.将关键词与主题词表合并
        articleProfile = articleKeywords.join(articleTopics,articleKeywords.article_id ==
                                              articleTopics.article_id2).select(
                                            ["article_id","channel_id","keywords","topics"])
        articleProfile.write.insertInto("article_profile")

        del keywordsIndex
        del _articleKeywordsWeights
        del articleKeywords
        del articleTopics
        gc.collect()

        logger.info("INFO:compute article_profile complete")
        return articleProfile

    def compute_article_similar(self,articleProfile):
        """
        计算增量文章与历史文章的相似度
        :param articleProfile:
        :return:
        """
        from pyspark.ml.feature import Word2VecModel

        def avg(row):
            x = 0
            for v in row.vectors:
                x += v
            return row.article_id,row.channel_id,x / len(row.vectors)

        for channel_id,channel_name in CHANNEL_INFO.items():
            profile = articleProfile.filter('channel_id = {}'.format(channel_id))
            wv_model = Word2VecModel.load( "hdfs://hadoop1:9000/headlines/models/channel_%d_%s.word2vec" %
                                           (channel_id, channel_name))

            vectors = wv_model.getVectors()

            # 计算向量
            profile.registerTempTable("incremental")
            articleKeywordsWeights = self.spark.sql("select article_id,channel_id,keyword,weight from profile\
                                    LATERAL VIEW explode(keywords) as keyword,weight")

            articleKeywordsWeightsAndVectors = articleKeywordsWeights.join(vectors,
                                                                           vectors.word == articleKeywordsWeights.keyword,
                                                                           "inner")
            articleKeywordVectors = articleKeywordsWeightsAndVectors.rdd.map(
                lambda r:(r.article_id,r.channel_id,r.keyword,r.weight * r.vector)).toDF(
                ["article_id","channel_id","keyword","weightVector"])

            articleKeywordVectors.registerTemptable("Temptable")
            articleVector = self.spark.sql(
                "select article_id, min(channel_id) channel_id, collect_set(weightVector) vectors from temptable group by article_id"
            ).rdd.map(avg).toDF(["article_id","channel_id","articleVector"])

            # 写入数据库hive
            def toArray(row):
                return row.article,row.channel_id,[float(i) for i in row.articleVector.toArray()]

            articleVector = articleVector.rdd.map(toArray).toDF(["article_id","channel_id","articleVector"])
            articleVector.write.insertInto("article_vector")

            import gc
            del wv_model
            del vectors
            del articleKeywordsWeights
            del articleKeywordsWeightsAndVectors
            del articleKeywordVectors
            gc.collect()

            # 得到历史文章向量，转换成vector格式，使用LSH求相似文章
            from pyspark.ml.linalg import Vectors
            from pyspark.ml.feature import BucketedRandomProjectionLSH
            train = self.spark.sql("select * from article_vector where channel_id=%d" % channel_id)

            def _array_to_vector(row):
                return row.article_id,Vectors.dense(row.articleVector)

            train = train.rdd.map(_array_to_vector).toDF(["article_id","articleVector"])
            test = articleVector.rdd.map(_array_to_vector).toDF("article_id","articleVector")

            brp = BucketedRandomProjectionLSH(inputCol="articleVector",outputCol="hashes",bucketLength=1.0,
                                              seed=12345)
            model = brp.fit(train)
            similar = model.approxSimilarityJoin(test,train,2.0,distCol="EuclideanDistance")

            def save_hbase(partitions):
                import happybase
                pool = happybase.ConnectionPool(size=3, host='hadoop1')

                with pool.connection() as conn:
                    article_similar = conn.table('article_similar')
                    for row in partitions:
                        if row.datasetA.article_id == row.datasetB.article_id:
                            pass
                        else:
                            article_similar.put(str(row.datasetA.article_id).encode(),
                                                {'similar:{}'.format(row.datasetB.article_id).encode()
                                                 : b'%0.4f' % (row.EuclideanDistance)})

            similar.foreachPartition(save_hbase)
















