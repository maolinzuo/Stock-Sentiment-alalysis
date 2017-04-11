import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import time, os
import json
import random
#import kafka_producer, kafka_consumer

class TwitterClient(object):
    '''
    Generic Twitter Class for sentiment analysis.
    '''
    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens from the Twitter Dev Console
        # consumer_key = 'oDKDLROopWOudtZmVCz6bX4r4'
        # consumer_secret = 'a4bXwdppENH2UNuHIRk1swa26iRqlCnxGegM4cVyvFspIIpFr4'
        consumer_key = "utlxZzQmSyEFyV7bkvGiUL0kz"
        consumer_secret = "V8SfcipgV10qVhYRYNa2SYDa59t9AhlQVdGtthAKSXrtyG2S9u"
        access_token = "3018213812-Q3pcr2H3tricjOhRDBiIVEjLWgKGeQX3Yf7rAsv"
        access_token_secret = "R85oKLMeaxFpflxQHRTSFOfB0p0LAwWpHLmFoit5AXoQ2"

        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_token_secret)
            # create tweepy API object to fetch tweets
            self.api = tweepy.API(self.auth)

        except:
            print("Error: Authentication Failed")

    def clean_tweet(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) \
                                    |(\w+:\/\/\S+)", " ", tweet).split())

    def get_tweet_sentiment(self, tweet):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

    def get_tweets(self, query, count):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = {}
        parsed_tweets=[]
        c = tweepy.Cursor(self.api.search, q=query).items(count)
        directory=make_dir(query)
        save_file = open(os.path.join(directory,'{}.json'.format(query)), 'a')
        #producer = kafka_producer.kafka_producer()
        for tweet in c:
            # tweets['user']=tweet.user
            tweets['text']=tweet.text
            tweets['time'] = tweet.created_at
            #if tweets['place'] != None and tweets['place']['bounding_box']['coordinates'][0][0] != None and tweet['lang'] == 'en':
            if(tweet.coordinates is None):
                latitude=random.uniform(0.0, 180.0)
                longitude=random.uniform(0.0, 180.0)
                tweets['geo']=[latitude, longitude]
            else:
                tweets['geo']=tweet.coordinates
            print(tweets)
            # producer.send(msg=str(tweets))
            # save_file.write(json.dumps(str(tweets)))
            # save_file.write("\n")


        # for tweet in c:
        #     print(type(tweet))
        #     print(type(tweet.coordinates))
        #     if(tweet.coordinates is None):
        #         latitude=random.uniform(0.0, 180.0)
        #         longitude=random.uniform(0.0, 180.0)
        #         tweet.coordinates=latitude
        #         print("Add random geo", tweet.coordinates)
        #     print(tweet)
        #     save_file.write(json.dumps(tweet._json))
        #     save_file.write("\n")




        # while True:
        #     try:
        #         tweet = c.next()
        #         # call twitter api to fetch tweets and parse tweets one by one
        #         # empty dictionary to store required params of a tweet
        #         parsed_tweet = {}
        #
        #         # saving text of tweet
        #         parsed_tweet['text'] = tweet.text
        #         # saving sentiment of tweet
        #         parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet.text)
        #         # saving the created time of tweet
        #         #parsed_tweet['time'] = tweet.created_at
        #
        #         # appending parsed tweet to tweets list
        #         #tweets.append(parsed_tweet)
        #         tweets.append(tweet)
        #
        #         if tweet.retweet_count > 0:
        #             # if tweet has retweets, ensure that it is appended only once
        #             if parsed_tweet not in tweets:
        #                 tweets.append(parsed_tweet)
        #         else:
        #             tweets.append(parsed_tweet)
        #
        #         # return parsed tweets
        #         if len(tweets) == count:
        #             return tweets
        #
        #     except tweepy.TweepError as e:
        #         print("Error : " + str(e))
        #         print("please wait for 1 mins...")
        #         time.sleep(60 * 1)
        #         continue

def make_dir(query):
    DIR="tweets"
    #Create main directory if necessary
    if not os.path.exists(DIR):
           os.mkdir(DIR)
    DIR = os.path.join(DIR, query)
    #Make sub-directory if necessary
    if not os.path.exists(DIR):
           os.mkdir(DIR)
    return DIR

def on_data(query, tweet, dir):
    save_file = open(os.path.join(dir,'{}.json'.format(query)), 'a')
    for i in tweet:
        save_file.write(json.dumps(i._json))
    print("*")*60
    print tweet
    print("*")*60
    #save_file.write(str(tweet))

# def main():
#     # creating object of TwitterClient Class
#     api = TwitterClient()
#     # calling function to get tweets
#     #search_input = raw_input("Please input the content you want to search: ")
#     #cnt = raw_input("Please input the quantity of the tweets you want to search: ")
#     #geo=raw_input("Please input the geo informatin latitide,longitude,radius(km/mile) ")
#     tweets = api.get_tweets(query ="China", count =10)

    # directory=make_dir(search_input)
    # on_data(query=search_input, tweet=tweets,dir=directory)









    #
    # # picking positive tweets from tweets
    # ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
    # # percentage of positive tweets
    # print("Positive tweets percentage: {} %".format(100*len(ptweets)/len(tweets)))
    # # picking negative tweets from tweets
    # ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
    # # percentage of negative tweets
    # print("Negative tweets percentage: {} %".format(100*len(ntweets)/len(tweets)))
    # # percentage of neutral tweets
    # print("Neutral tweets percentage: {} % ".format(100*(len(tweets) - len(ntweets) - len(ptweets))/len(tweets)))
    #
    # print(len(tweets))
    # print(len(ptweets))
    # print(len(ntweets))
    # # print positive tweets
    # print("\n\nPositive tweets:")
    # for tweet in ptweets[:-1]:
    #     print(tweet['text'])
    #     print(tweet['time'])
    #     print('-------------------')
    #
    # # print negative tweets
    # print("\n\nNegative tweets:")
    # for tweet in ntweets[:-1]:
    #     print(tweet['text'])
    #     print(tweet['time'])
    #     print('-------------------')
#
# if __name__ == "__main__":
#     # calling main function
#     main()
