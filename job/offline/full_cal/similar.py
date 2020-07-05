import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))
from offline import SparkSessionBase
from pyspark.ml.feature import Word2Vec


class TrainWord2VecModel(SparkSessionBase):
    """
    训练word2vec模型并保存
    """
    SPARK_APP_NAME = "Word2Vec"
    ENABLE_HIVE_SUPPORT = True
    SPARK_EXECUTOR_MEMORY = "4g"

    def __init__(self):
        self.spark = self._create_spark_session()

w2v = TrainWord2VecModel()

# 训练一个频道的模型
w2v.spark.sql("use article")

article_data =w2v.spark.sql("select * from article_data where channel_id=17 limit 10")
# article_data.show()

# 分词
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

words_df = article_data.rdd.mapPartitions(segmentation).toDF(["article_id","channel_id","words"])
words_df.show()

# 训练17#频道的word2vec
# w2v_model = Word2Vec(vectorSize=100,inputCol='words',outputCol='model',minCount=3)
# model = w2v_model.fit(words_df)
# model.save("hdfs://hadoop1:9000/headlines/models/word2vec_model_17")

# 1.加载模型，得到每个词的向量
from pyspark.ml.feature import Word2VecModel
wv = Word2VecModel.load("hdfs://hadoop1:9000/headlines/models/word2vec_model_17")
vectors = wv.getVectors()
vectors.show()

# 2.获取该频道的文章画像，得到文章画像的关键词，获取这20个关键词对应的词向量
article_profile = w2v.spark.sql("select * from article_profile where channel_id=17 limit 10")

# 3.计算得到文章每个词的向量,利用explode将keywords字典炸开得到keyword和weight
article_profile.registerTempTable('profile')
keyword_weight = w2v.spark.sql("select article_id,channel_id,keyword,weight from profile "
                               "LATERAL VIEW explode(keywords) as keyword,weight")
keyword_weight.show()

# 4.将文章画像的keyword_weight和w2v模型合并，得到每篇文章20个关键词的词向量
_keywords_vector = keyword_weight.join(vectors,vectors.word==keyword_weight.keyword,how='inner')
_keywords_vector.show()

def compute_vector(row):
    return row.article_id,row.channel_id,row.keyword,row.weight * row.vector
articleKeyVectors = _keywords_vector.rdd.map(compute_vector).toDF(["article_id","channel_id",
                                                                   "keyword","keywordVector"])
articleKeyVectors.show()

articleKeyVectors.registerTempTable("temptable")
articleKeyVectors = w2v.spark.sql("select article_id,min(channel_id) channel_id,collect_set(keywordVector) vectors\
                                    from temptable group by article_id")
articleKeyVectors.show()

# 计算每篇文章的平均词向量即文章向量(20个词的向量和除以20)
def compute_avg_vectors(row):
    x = 0
    for i in row.vectors:
        x += i
    return row.article_id, row.channel_id, x/len(row.vectors)

article_vector = articleKeyVectors.rdd.map(compute_avg_vectors).toDF(["article_id","channel_id","vector"])
article_vector.show()

# 因为得到的文章向量是vector格式，但是hive不支持vector格式，因此要把这一列转化为数组array格式
def toArray(row):
    return row.article_id,row.channel_id,[float(i) for i in row.vector.toArray()]
article_vector = article_vector.rdd.map(toArray).toDF(["article_id","channel_id","vector"])
# article_vector.write.insertInto("article_vector")

# 利用LSH计算文章相似度
# 1.读取数据，将文章向量从array转换成vector
from pyspark.ml.linalg import Vectors

def toVector(row):
    return row.article_id,Vectors.dense(row.vector)

train = article_vector.rdd.map(toVector).toDF(["article_id","vector"])

# 计算相似的文章
from pyspark.ml.feature import BucketedRandomProjectionLSH

brp = BucketedRandomProjectionLSH(inputCol='vector',outputCol='hashes',numHashTables=4.0,bucketLength=10.0)
model = brp.fit(train)
similar = model.approxSimilarityJoin(train,train,2.5,distCol='EuclideanDistance')
# 按欧式距离从小到大排序，距离越小越相似
similar.sort(['EuclideanDistance'])
similar.show()
# 得到的similar是一个struct结构体，因此下面要用row.datasetA.article_id表示

# 相似文章结果存入hbase
# def save_hbase(partitions):
#     import happybase
#     pool = happybase.ConnectionPool(size=3,host='hadoop1')
#
#     with pool.connection() as conn:
#         article_similar = conn.table('article_similar')
#         for row in partitions:
#             if row.datasetA.article_id == row.datasetB.article_id:
#                 pass
#             else:
#                 article_similar.put(str(row.datasetA.article_id).encode(),
#                                     {'similar:{}'.format(row.datasetB.article_id).encode()
#                                      :b'%0.4f' % (row.EuclideanDistance)})
# similar.foreachPartition(save_hbase)








