import json
from tweepy.streaming import StreamListener
from pymongo import MongoClient
#from awses.connection import AWSConnection
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch

import datetime

# import twitter keys and tokens
from config import *


es=Elasticsearch()
# add tweets to mongodb
client = MongoClient('localhost', 27017)
db=client['Valkyrie-BigData']
collection=db['Tweetsentiment']
# create instance of elasticsearch
#es = Elasticsearch()
class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):

        # decode json/read json
        dict_data = json.loads(data)

        # pass tweet into TextBlob
        tweet = TextBlob(dict_data["text"])

        # output sentiment polarity
        print tweet.sentiment.polarity

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # output sentiment
        print sentiment
        dt=datetime.datetime.today().strftime('%Y-%m-%d')
        print dt 
        print dict_data["created_at"]
        
        #insert data to mongodb collection
        obj = {"author":dict_data["user"]["screen_name"],"date":dt,"message":dict_data["text"],"polarity":tweet.sentiment.polarity,"subjectivity": tweet.sentiment.subjectivity,"sentiment": sentiment}
        
        tweetind=collection.insert_one(obj).inserted_id

        print obj
        
        # add text and sentiment info to elasticsearch
        es.index(index="sentiment",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dt,
                       "message": dict_data["text"],
                       "polarity": tweet.sentiment.polarity,
                       "subjectivity": tweet.sentiment.subjectivity,
                       "sentiment": sentiment})
        
        return True

    # on failure
    def on_error(self, status):
        print status

# python main method
if __name__ == '__main__':

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for "listed technologies" keyword
    stream.filter(track=['#JS','#java','#python','#Ruby','#swift','#mongoDB','#Ruby','#android'])