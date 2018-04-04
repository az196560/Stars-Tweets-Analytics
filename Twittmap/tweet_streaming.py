import tweepy
import json
import time
import re
from elasticsearch import Elasticsearch

consumer_token = '***'
consumer_secret = '***'
access_token = '***'
access_token_secret = '***'

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
es = Elasticsearch([{'host': '***.us-east-1.es.amazonaws.com', 'port': 80}])

file = open('twitter1.json', 'a')
tweets = []

class TweetStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.num_tweets = 0
        self.file = tweets
    def on_data(self, data):
        tw_obj = json.loads(data)
        try:
            if tw_obj["coordinates"] is not None:
                print(tw_obj["coordinates"])
                es.index(index = "twitter",
                         doc_type = "tweets",
                         body = {"text": tw_obj["text"],
                                 "name": tw_obj["user"]["name"],
                                 "time": tw_obj["created_at"],
                                 "profile": tw_obj["user"]["profile_image_url"],
                                 "location": tw_obj["coordinates"]["coordinates"]})
        except:
            pass
    def on_error(self, status_code):
        if status_code == 420:
            return False

tweetStreamListener = TweetStreamListener()
tweetStream = tweepy.Stream(auth = api.auth, listener=tweetStreamListener)
tweetStream.filter(track = ["study", "eat", "trip", "sports", "TV", "gaming", "party", "work", "hiking", "sleep"])
