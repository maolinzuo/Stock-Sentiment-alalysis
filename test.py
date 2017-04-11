from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
#from pyspark.ml.feature import HashingTF, IDF, Tokenizer

conf = SparkConf().setAppName("appName").setMaster("local")
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
script_dir = os.path.dirname(__file__)
#training = "training.1600000.processed.noemoticon.csv"
#testing="testdata.manual.2009.06.14.csv"
filename="train.csv"
#testing="test.csv"
abs_file_path = os.path.join(script_dir, filename)
#abs_file_path_testing= os.path.join(script_dir, testing)
raw_data = sc.textFile(abs_file_path)
#rawTestingData=sc.textFile(abs_file_path_testing)
label_text=raw_data.map(lambda x:(float(x.split(",")[0][1]), x.split(",")[5].encode('ascii','ignore')))
hashingTF = HashingTF()
# test=rawTestingData.map(lambda x:(float(x.split(",")[0][1]), x.split(",")[5].encode('ascii','ignore')))
feature_htf = label_text.map(lambda tup: hashingTF.transform(tup[1]))
feature_idf= IDF().fit(feature_htf)
featured= feature_idf.transform(feature_htf)

label=label_text.map(lambda tup: tup[0])
featured_idx=featured.zipWithIndex()
label_idx=label.zipWithIndex()
idx_featured=featured_idx.map(lambda x:(x[1],x[0]))
idx_label=label_idx.map(lambda x:(x[1],x[0]))


label_feature=idx_label.join(idx_featured).map(lambda x:x[1])


label_feature_LabeledPoint=label_feature.map(lambda x:(LabeledPoint(x[0],x.SparseVector)))
for i in label_feature_LabeledPoint.collect():
    print i


# model=NaiveBayes.train(label_feature_LabeledPoint,1)
# print("Done!")
# labelpoint=label_feature.map(lambda x:LabeledPoint(x[0],x[1]))
# for i in labelpoint.collect():
#     print i
# print(type(labelpoint))
# neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
# # for i in neg.collect():
# print(type(neg))
#     print i
##training, test = featured.randomSplit([0.7, 0.3], seed=0)


# neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
# print(type(neg))

# print("Done!")






































#
# #training_df=spark.createDataFrame(training,["label", "sentence"])
# training_df=training.toDF(["label", "sentence"])
# testing_df=testing.toDF(["label", "sentence"])
# tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
# train_wordsData = tokenizer.transform(training_df)
# test_wordsData=tokenizer.transform(testing_df)
# hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=5000)
# train_featurizedData = hashingTF.transform(train_wordsData)
# test_featurizedData = hashingTF.transform(test_wordsData)
# idf = IDF(inputCol="rawFeatures", outputCol="features")
# train_idfModel = idf.fit(train_featurizedData)
# test_idfModel = idf.fit(test_featurizedData)
# train_rescaledData = train_idfModel.transform(train_featurizedData)
# test_rescaledData = test_idfModel.transform(test_featurizedData)
#
# labeled_training_data=train_rescaledData.rdd
# #
# for i in labeled_training_data.collect():
#     print i.label, i.features
#
# # print("*")*10000
# #training_data = labeled_training_data.map(lambda k: LabeledPoint(k.label, k.features))
# training_data = labeled_training_data.map(lambda k: LabeledPoint(k.label, k.features))
# for i in training_data.collect():
#     print i
    # print(type(i[1]))
#model=NaiveBayes.train(training_data,1)

# tfidf_idx = tfidf_training.zipWithIndex()
# training_idx = training.zipWithIndex()
#
#
#
# #get the index and label
# idx_training = training_idx.map(lambda line: (line[1], line[0]))
#
# idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
# joined_tfidf_training = idx_training.join(idx_tfidf)
# labeled_training_data = training_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))


# for i in training.collect():
#     print i[0]
#     print i[1]
#

#
# hashingTF = HashingTF()
# tf_training = training.map(lambda tup: hashingTF.transform(tup[1]))
# # for i in tf_training.collect():
# #     print i
#
#
#
# idf_training = IDF().fit(tf_training)
# tfidf_training = idf_training.transform(tf_training)
# tfidf_idx = tfidf_training.zipWithIndex()
# training_idx = training.zipWithIndex()
#
#
#
# #get the index and label
# idx_training = training_idx.map(lambda line: (line[1], line[0]))
#
# idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
# joined_tfidf_training = idx_training.join(idx_tfidf)
#
# training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
# labeled_training_data = training_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
#
#
# # for i in labeled_training_data.collect():
# #     print i
# #
# #
# model=NaiveBayes.train(labeled_training_data,0.7)
# #
# test=rawTestingData.map(lambda x:(float(x.split(",")[0][1]), x.split(",")[5].encode('ascii','ignore')))
# tf_test = test.map(lambda tup: hashingTF.transform(tup[1]))
# idf_test = IDF().fit(tf_test)
# tfidf_test = idf_test.transform(tf_test)
# tfidf_idx = tfidf_test.zipWithIndex()
# test_idx = test.zipWithIndex()
# idx_test = test_idx.map(lambda line: (line[1], line[0]))
# idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
# joined_tfidf_test = idx_test.join(idx_tfidf)
# test_labeled = joined_tfidf_test.map(lambda tup: tup[1])
# labeled_test_data = test_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
#
# # for i in labeled_test_data.collect():
# #     print i
#
# predictionAndLabel = labeled_test_data.map(lambda p : (model.predict(p.features), p.label))
#
# accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / labeled_test_data.count()
# print accuracy
#
#
# for i in predictionAndLabel.collect():
#     print i
# # labeled_2 = test_labeled.map(lambda k: (k[0][1], LabeledPoint(k[0][0], k[1])))
# # predictionAndLabel2 = labeled_2.map(lambda p : [p[0], model.predict(p[1].features), p[1].label])
# # accuracy = 1.0 * predictionAndLabel2.filter(lambda (x, v): x == v).count() / labeled_test_data.count()
# # print accuracy
#
# # for line in training_labeled.collect():
# #     print("lable:", line[0], "text: ",line[1])
