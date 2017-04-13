# coding: utf-8

from __future__ import division
import datetime
import requests
import json
from textblob import TextBlob
import re
import os
import warnings
warnings.filterwarnings('ignore')

api_key = "AIzaSyDOwLCRuAmni2tAy0uLHOtV4KZp_Ic46gI"
COMMENT_URL_BASE = "https://www.googleapis.com/youtube/v3/commentThreads?part=snippet"


#Preparing The HTTP Request
#result_num = int(raw_input("Please indicate the quantity of the videos that you want to search: "))

keyword = 'apple'
inputnum = 10

def build_url(baseurl, parameterlist):
    for parameter in parameterlist:
        baseurl = baseurl + "&" + parameter + "=" + parameterlist[parameter]    
    return baseurl

def get_response(url):
    response = requests.get(url)
    response_json = json.loads(response.text)
    return response_json

def get_desired_field(json1, fieldarray):
    if len(fieldarray) is not 0:
        field = fieldarray[0]
        del fieldarray[0]
        return get_desired_field(json1[field], fieldarray)
    else:
        return json1
    
def clean_comment(comment):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])                                     |(\w+:\/\/\S+)", " ", comment).split())
    
def get_comment_sentiment(comment):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(clean_comment(comment))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'


if __name__ == '__main__':
    f = open('sentiment.txt', 'w')
    f.write('date pos_count neg_count netural_count')
    interval = datetime.timedelta(days = 1)
    time_after = datetime.date(2016,7,10)
    time_before = time_after + interval
    #time_period = str(time_after) + ' to ' + str(time_before)
    # while True:

    # #     time_period = raw_input("Please indicate the time period of the video (YYYY-MM-DD to YYYY-MM-DD): ")
    # matchObj = re.match( r'(\d{4}-\d{2}-\d{2}) to (\d{4}-\d{2}-\d{2})', time_period)
    # try:
    #     time_after = matchObj.group(1)
    #     time_before = matchObj.group(2)
    #         print "time_after: " + time_after
    #         print "time_before: " + time_before
    #         break
            
    # except AttributeError:
    #     print 'Unable to identify the date.'
    #         print "Your input is wrong! Please try again."

    while not time_before == datetime.date(2016,10,9):

        parameters = {"part": "snippet",
                      "maxResults": inputnum, # Set the number of the results here
                      "order": "date",
                      "pageToken": "",
                      "publishedAfter": str(time_after) + "T00:00:00Z",
                      "publishedBefore": str(time_before) + "T00:00:00Z",
                      "q": "",
                      "key": api_key,
                      "type": "video",
                      }
        url = "https://www.googleapis.com/youtube/v3/search"

        parameters["q"] = keyword
        page = requests.request(method="get", url=url, params=parameters)
        j_results = json.loads(page.text)

        videoIds = []
        comments = []
        sentiments = []

        for i in j_results["items"]:
            videoIds.append(get_desired_field(i, ["id", "videoId"]))

        print videoIds

        for vid_id in videoIds:
                parameters_list = {"key": api_key, "videoId": vid_id}
                url = build_url(COMMENT_URL_BASE, parameters_list)
                r = get_response(url)
                while True:
                    if "items" in r:  # comments are disabled for some videos, in that case response is empty
                        for j in get_desired_field(r, ["items"]):
                            try:
                                tmp_comment = get_desired_field(j, ["snippet", "topLevelComment", "snippet", "textDisplay"])
                                comments.append(clean_comment(tmp_comment))
                                sentiment = get_comment_sentiment(tmp_comment)
                                sentiments.append(sentiment)
                            except UnicodeEncodeError:
                                pass
                    if "nextPageToken" not in r:
                        break
                    else:
                        parameters_list = {"key": api_key, "videoId": vid_id, "pageToken": r["nextPageToken"]}
                        url = build_url(COMMENT_URL_BASE, parameters_list)
                        r = get_response(url)
                    
                    positive_sentiments = [sentiment for sentiment in sentiments if sentiment == "positive"]
                    negative_sentiments = [sentiment for sentiment in sentiments if sentiment == "negative"]
        try:            
            total_count = len(comments)
            positive_count = len(positive_sentiments)
            negative_count = len(negative_sentiments)
            neutral_count = total_count - positive_count - negative_count

            positive_rate = positive_count/total_count
            negative_rate = negative_count/total_count
            print str(time_after)
            print ("Positive comments percentage: {} %".format(100*positive_rate))
            print ("Negative comments percentage: {} %".format(100*negative_rate))
            print (positive_count,positive_rate, negative_count, negative_rate, neutral_count, len(comments))
            f.write(str(time_after) + " " + str(positive_count) + " " + str(negative_count) + " " + str(neutral_count) + '\n')
        except:
            pass

        time_after = time_after + interval
        time_before = time_after + interval
