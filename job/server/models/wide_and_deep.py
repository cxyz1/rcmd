import tensorflow as tf

class WDL(object):
    """
    wide&deep模型训练
    """
    def __init__(self):
        pass

    @staticmethod
    def read_ctr_tfrecords():

        def parse_tfrecords_function(example_proto):
            """
            解析tfrecords中的每个example
            :param example_proto:每个样本
            :return:
            """
            features = {
                "label":tf.FixedLenFeature([],tf.int64),
                "feature":tf.FixedLenFeature([],tf.string)
            }
            parsed_features = tf.parse_single_example(example_proto,features)

            # 修改其中的特征类型和形状
            # 解码
            feature = tf.decode_raw(parsed_features['feature'],tf.float64)
            feature = tf.reshape(tf.cast(feature,tf.float32),[1,121])

            # 特征顺序 1 channel_id,  100 article_vector, 10 user_weights, 10 article_weights
            # 1 channel_id类别型特征， 100维文章向量求平均值当连续特征，10维用户权重求平均值当连续特征，10维文章权重求平均值当连续特征
            channel_id = tf.cast(tf.slice(feature,[0,0],[1,1]),tf.int32)
            vector = tf.reduce_sum(tf.slice(feature,[0,1],[1,100]),axis=1)
            user_weights = tf.reduce_sum(tf.slice(feature,[0,101],[1,10]),axis=1)
            article_weights = tf.reduce_sum(tf.slice(feature,[0,111],[1,10]),axis=1)

            # 4个特征值构造字典
            FEATURE_COLUMN = ['channel_id','vector','user_weights','article_weights']
            tensor_list = [channel_id,vector,user_weights,article_weights]
            feature_dict = dict(zip(FEATURE_COLUMN,tensor_list))

            label = tf.cast(parsed_features['label'], tf.float32)

            return feature_dict,label

        dataset = tf.data.TFRecordDataset(["./train_ctr_20200630.tfrecords"])
        dataset = dataset.map(parse_tfrecords_function)
        dataset = dataset.batch(64)
        dataset = dataset.repeat()
        return dataset

    def build_estimator(self):
        """
        wide&deep模型训练
        wide侧只是离散特征，deep侧离散与连续都得有，但是deep侧的离散特征必要embedding
        :return:
        """
        # 1.指定wide侧的feature_column（离散分类）
        # wide侧的特征如果是chanel_id这种，就是具体类别的数字
        # num_buckets是离散分类的类别个数，必须写
        channel_id = tf.feature_column.categorical_column_with_identity('channel_id',num_buckets=25)

        wide_columns = [channel_id]

        # 2.指定deep侧的feature_column(连续特征)
        vector = tf.feature_column.numeric_column('vector')
        user_weights = tf.feature_column.numeric_column('user_weights')
        article_weights = tf.feature_column.numeric_column('article_weights')

        # deep侧的离散特征要先embedding
        deep_columns = [tf.feature_column.embedding_column(channel_id,dimension=25),
                        vector,user_weights,article_weights]
        # 3.模型训练并保存
        model = tf.estimator.DNNLinearCombinedClassifier(model_dir="./ckpt/wide_and_deep",
                                                             linear_feature_columns=wide_columns,
                                                             dnn_feature_columns=deep_columns,
                                                             dnn_hidden_units=[1024,512,256])

        # 4.模型导出
        columns = wide_columns + deep_columns
        feature_spec = tf.feature_column.make_parse_example_spec(columns)
        serving_input_receiver_fn =tf.estimator.export.build_parsing_serving_input_receiver_fn(feature_spec)
        model.export_savedmodel("./serving_model/wdl",serving_input_receiver_fn)

        return model

if __name__ == '__main__':
    wdl = WDL()
    model = wdl.build_estimator()
    model.train(input_fn=wdl.read_ctr_tfrecords,steps=10)
    # result = model.evaluate(input_fn=wdl.read_ctr_tfrecords)
    # print(result)