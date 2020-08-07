from offline import SparkSessionBase
from pyspark import SparkContext

class OriginArticleData(SparkSessionBase):


    SPARK_APP_NAME = "mergeArticle"
    SPARK_URL = "local"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

oa = OriginArticleData()

oa.spark.sql("use article")
basic_content = oa.spark.sql("select * from article_data limit 10")
# basic_content.show()

def segmentation(partition):
    """
    对每篇文章分词
    :param partition:
    :return:
    """
    import os
    import re

    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    import codecs

    abspath = "/root/words"

    # 结巴加载用户词典
    # userDict_path = os.path.join(abspath, "ITKeywords.txt")
    # jieba.load_userdict(userDict_path)

    # 停用词文本
    stopwords_path = os.path.join(abspath, "stopwords.txt")

    def get_stopwords_list():
        """返回stopwords列表"""
        stopwords_list = [i.strip()
                          for i in codecs.open(stopwords_path).readlines()]
        return stopwords_list

    # 所有的停用词列表
    stopwords_list = get_stopwords_list()

    # 分词
    def cut_sentence(sentence):
        """对切割之后的词语进行过滤，去除停用词，保留名词，英文和自定义词库中的词，长度大于2的词"""
        # print(sentence,"*"*100)
        # eg:[pair('今天', 't'), pair('有', 'd'), pair('雾', 'n'), pair('霾', 'g')]
        seg_list = pseg.lcut(sentence)
        seg_list = [i for i in seg_list if i.flag not in stopwords_list]
        filtered_words_list = []
        for seg in seg_list:
            # print(seg)
            if len(seg.word) <= 1:
                continue
            elif seg.flag == "eng":
                if len(seg.word) <= 2:
                    continue
                else:
                    filtered_words_list.append(seg.word)
            elif seg.flag.startswith("n"):
                filtered_words_list.append(seg.word)
            elif seg.flag in ["x", "eng"]:  # 是自定一个词语或者是英文单词
                filtered_words_list.append(seg.word)
        return filtered_words_list

    for row in partition:
        sentence = re.sub("<.*?>", "", row.sentence)    # 替换掉标签数据
        words = cut_sentence(sentence)
        yield row.article_id, row.channel_id, words

words_df = basic_content.rdd.mapPartitions(segmentation).toDF(["article_id","channel_id","words"])
words_df.show()


from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import CountVectorizerModel
from pyspark.ml.feature import IDF
from pyspark.ml.feature import IDFModel

# cv模型训练与保存
# cv =CountVectorizer(inputCol='words',outputCol='countFeatures',vocabSize=20000,minDF=1.0)
# cv_model = cv.fit(words_df)
# cv_model.write().overwrite().save("hdfs://hadoop1:9000/headlines/models/cv.model")

# 加载cv_model，训练idf模型,保存idf模型
# cv_model =CountVectorizerModel.load("hdfs://hadoop1:9000/headlines/models/cv.model")
# cv_result = cv_model.transform(words_df)
# cv_result.show()
#
# idf = IDF(inputCol="countFeatures",outputCol="idfFeatures")
# idfModel =idf.fit(cv_result)
# idfModel.write().overwrite().save("hdfs://hadoop1:9000/headlines/models/idf.model")


# 得到每个词与索引的对应关系
# cv_model = CountVectorizerModel.load("hdfs://hadoop1:9000/headlines/models/cv.model")
# idf_model = IDFModel.load("hdfs://hadoop1:9000/headlines/models/idf.model")

# keywords_list_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))
#
# def func(row):
#     for index in range(len(row)):
#         row[index] = list(row[index])
#         row[index].append(index)
#         row[index][1] = float(row[index][1])
#
# func(keywords_list_with_idf)
# sc = oa.spark.sparkContext
# rdd = sc.parallelize(keywords_list_with_idf)
# df = rdd.toDF(["keywords", "idf", "index"])
# df.show()
# df.write.insertInto('idf_keywords_values')

# 计算tfidf值，并保存在hive
# cv_result = cv_model.transform(words_df)
# tfidf_result = idf_model.transform(cv_result)
#
# 取topk个tfidf值大的结果
# def func(partition):
#     TOPK = 20
#     for row in partition:
#         _ = list(zip(row.idfFeatures.indices,row.idfFeatures.values))
#         _ = sorted(_,key=lambda x:x[1],reverse=True)
#         result = _[:TOPK]
#         for words_index,tfidf in result:
#             yield row.article_id,row.channel_id,int(words_index),round(float(tfidf),4)
#
# _keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(["article_id","channel_id","index","tfidf"])
# _keywordsByTFIDF.show()
#
# # 将idf_keywords_values与_keywordsByTFIDF合并，得到每个词与tfidf的对应关系
# keywordsIndex = oa.spark.sql("select keyword,index idx from idf_keywords_values")
# keywordsByTFIDF = _keywordsByTFIDF.join(keywordsIndex,keywordsIndex.idx == _keywordsByTFIDF.index).\
#                     select(["article_id","channel_id","keyword","tfidf"])
# keywordsByTFIDF.show()
# keywordsByTFIDF.write.insertInto("tfidf_keywords_values")


# textrank
# 分词
def textrank(partition):
    import os

    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    import codecs

    abspath = "/root/words"

    # 结巴加载用户词典
    # userDict_path = os.path.join(abspath, "ITKeywords.txt")
    # jieba.load_userdict(userDict_path)

    # 停用词文本
    stopwords_path = os.path.join(abspath, "stopwords.txt")

    def get_stopwords_list():
        """返回stopwords列表"""
        stopwords_list = [i.strip()
                          for i in codecs.open(stopwords_path).readlines()]
        return stopwords_list

    # 所有的停用词列表
    stopwords_list = get_stopwords_list()

    class TextRank(jieba.analyse.TextRank):
        def __init__(self, window=20, word_min_len=2):
            super(TextRank, self).__init__()
            self.span = window  # 窗口大小
            self.word_min_len = word_min_len  # 单词的最小长度
            # 要保留的词性，根据jieba github ，具体参见https://github.com/baidu/lac
            self.pos_filt = frozenset(
                ('n', 'x', 'eng', 'f', 's', 't', 'nr', 'ns', 'nt', "nw", "nz", "PER", "LOC", "ORG"))

        def pairfilter(self, wp):
            """过滤条件，返回True或者False"""

            if wp.flag == "eng":
                if len(wp.word) <= 2:
                    return False

            if wp.flag in self.pos_filt and len(wp.word.strip()) >= self.word_min_len \
                    and wp.word.lower() not in stopwords_list:
                return True
    # TextRank过滤窗口大小为5，单词最小为2
    textrank_model = TextRank(window=5, word_min_len=2)
    allowPOS = ('n', "x", 'eng', 'nr', 'ns', 'nt', "nw", "nz", "c")

    for row in partition:
        tags = textrank_model.textrank(row.sentence, topK=20, withWeight=True, allowPOS=allowPOS, withFlag=False)
        for tag in tags:
            yield row.article_id, row.channel_id, tag[0], tag[1]

textrank_keywords_df = basic_content.rdd.mapPartitions(textrank).toDF(["article_id","channel_id","keyword","textrank"])
textrank_keywords_df.show()
# textrank_keywords_df.write.insertInto("textrank_keywords_values")

# 文章画像article_profile
# keywords的权重用idf*textrank得到，而topic用textrank的topk个词
# articel_id, channel_id,  keywords,    topic
#   1           18      {python:1.898,} [java,学习,]
idf_keywords_values = oa.spark.sql("select * from idf_keywords_values")
idf_keywords_values.show()

keywords_res = textrank_keywords_df.join(idf_keywords_values,on=['keyword'],how='left')
keywords_res.show()

keywords_weights = keywords_res.withColumn('weights', keywords_res.textrank * keywords_res.idf).select\
                                (["article_id", "channel_id", "keyword", "weights"])
keywords_weights.show()

# 因为idf值只算了前20个，而textrank算了全部，所以有缺失值，要把none删除
keywords_weights.dropna().show()

# 将关键词和权重合并成字典
keywords_weights.registerTempTable("temp")
keywords_weights = oa.spark.sql("select article_id,min(channel_id) channel_id,collect_list(keyword) \
                                keyword,collect_list(weights) weights from temp group by article_id")
keywords_weights.show()

def _func(row):
    return row.article_id,row.channel_id,dict(zip(row.keyword,row.weights))
article_words =keywords_weights.rdd.map(_func).toDF(["article_id","channel_id","keywords"])
article_words.show()

# 将tfidf和textrank共现的词作为主题词
topic_words = "select t.article_id article_id2, collect_set(t.keyword) topics from tfidf_keywords_values\
                t inner join textrank_keywords_values r where t.keyword=r.keyword group by article_id2"
article_topic = oa.spark.sql(topic_words)
article_topic.show()

# 将关键词和主题词合并得到文章画像
article_profile =article_words.join(article_topic,article_words.article_id==article_topic.article_id2).\
                    select(["article_id","channel_id","keywords","topics"])
article_profile.show()
# article_profile.write.insertInto("article_profile",overwrite=True)

