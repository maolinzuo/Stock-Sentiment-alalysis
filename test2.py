import NaiveBayes
#from pyspark.mllib.classification import NaiveBayes
#from pyspark.mllib.linalg import Vectors
#from pyspark.mllib.regression import LabeledPoint
#from pyspark.mllib.feature import HashingTF
#from pyspark.mllib.feature import IDF
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# conf = SparkConf().setAppName("appName").setMaster("local")
# conf.set("spark.executor.memory", "2g")
# sc = SparkContext(conf=conf)
# spark = SparkSession(sc)


classifier=NaiveBayes.Naive_Bayes()
model=classifier.train()


#
# sentence=[]
# sentence.append("I really hate rain! It makes me feel sick!")
# sentence.append("Sunny day! Feeling warm and pleased!")
#
# for i in sentence:
#     print(i, "rediction: ", model.predict(i))
