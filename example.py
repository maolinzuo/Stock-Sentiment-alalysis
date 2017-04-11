from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark import SparkConf, SparkContext
import os
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("appName").setMaster("local")
conf.set("spark.executor.memory", "2g")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

conf = SparkConf().setAppName("appName").setMaster("local")
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


sentenceData = spark.createDataFrame([
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic regression models are neat")
], ["label", "sentence"])


tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
for i in wordsData.collect():
    print i
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")

idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
for features_label in rescaledData.select("features", "label").take(3):
  print(features_label)
