from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
import getTweet





conf = SparkConf().setAppName("appName").setMaster("local")
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def parseTweet(line):
    parts = line.split(',')
    label = float(parts[0][1])
    tweet = parts[5]
    words = tweet.strip().split(" ")
    return (label, words)


def vectorize(training):
    hashingTF = HashingTF()
    tf_training = training.map(lambda tup: hashingTF.transform(tup[1]))
    idf_training = IDF().fit(tf_training)
    tfidf_training = idf_training.transform(tf_training)
    tfidf_idx = tfidf_training.zipWithIndex()
    training_idx = training.zipWithIndex()
    idx_training = training_idx.map(lambda line: (line[1], line[0]))
    idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
    joined_tfidf_training = idx_training.join(idx_tfidf)
    training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
    labeled_training_data = training_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
    return labeled_training_data

def train():
    print("Training..........")
    script_dir = os.path.dirname(__file__)
    #filename_training="training.1600000.processed.noemoticon.csv"
    #filename_testing="training.1600000.processed.noemoticon.csv"
    filename_training="testdata.manual.2009.06.14.csv"
    training_file = os.path.join(script_dir, filename_training)
    allData = sc.textFile(training_file)
    header = allData.first()
    data = allData.filter(lambda x: x != header).map(parseTweet)
    training, test = data.randomSplit([0.7, 0.3], seed=0)


    # hashingTF = HashingTF()
    # tf_training = training.map(lambda tup: hashingTF.transform(tup[1]))
    # idf_training = IDF().fit(tf_training)
    # tfidf_training = idf_training.transform(tf_training)
    # tfidf_idx = tfidf_training.zipWithIndex()
    # training_idx = training.zipWithIndex()
    # idx_training = training_idx.map(lambda line: (line[1], line[0]))
    # idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
    # joined_tfidf_training = idx_training.join(idx_tfidf)
    # training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
    # labeled_training_data = training_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))



    labeled_training_data=vectorize(training)
    labeled_test_data=vectorize(test)

    model = NaiveBayes.train(labeled_training_data, 1.0)



    # tf_test = test.map(lambda tup: hashingTF.transform(tup[1]))
    #
    # idf_test = IDF().fit(tf_test)
    #
    # tfidf_test = idf_test.transform(tf_test)
    #
    # tfidf_idx = tfidf_test.zipWithIndex()
    #
    # test_idx = test.zipWithIndex()
    #
    # idx_test = test_idx.map(lambda line: (line[1], line[0]))
    #
    # idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
    #
    # joined_tfidf_test = idx_test.join(idx_tfidf)
    #
    # test_labeled = joined_tfidf_test.map(lambda tup: tup[1])
    # labeled_test_data = test_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))



    predictionAndLabel = labeled_test_data.map(lambda p : (model.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / labeled_test_data.count()
    po_accuracy=1.0 * predictionAndLabel.filter(lambda (x, v): x == v and x==float(4)).count() / (labeled_test_data.filter(lambda x:x.label==float(4)).count()+1)
    ne_accuracy=1.0 * predictionAndLabel.filter(lambda (x, v): x == v and x==float(2)).count() / (labeled_test_data.filter(lambda x:x.label==float(2)).count()+1)
    na_accuracy=1.0 * predictionAndLabel.filter(lambda (x, v): x == v and x==float(0)).count() / (labeled_test_data.filter(lambda x:x.label==float(0)).count()+1)
    print accuracy
    print("positive accuracy:", po_accuracy)
    print("neural accuracy:", ne_accuracy)
    print("negative accuracy:", na_accuracy)
    print("Done training")
    return model

def main():
    #api=getTweet.TwitterClient()
    # calling function to get tweets
    # search_input = raw_input("Please input the content you want to search: ")
    # cnt = raw_input("Please input the quantity of the tweets you want to search: ")
    #geo=raw_input("Please input the geo informatin latitide,longitude,radius(km/mile) ")
    #tweets = api.get_tweets(query = "China", count = 10)
    train()

if __name__ == "__main__":
    # calling main function
    main()
