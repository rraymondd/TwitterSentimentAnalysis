from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pymongo import MongoClient
from textblob import TextBlob
import matplotlib.pyplot as plt
from mpld3 import fig_to_d3
import statistics
import json
import re
import numpy as np


access_token = "3166078670-kUTXvUAHyaddLaoWiibqT4aSYKhLAS2zy1I7pos"
access_token_secret = "ZcOHVTe4PwdDCmjAdmwGfepyJWI6eXpRouCwHVvaPyFLi"
consumer_key = "fFStHXn6NiOvunlkYF1ojGn8M"
consumer_secret = "4KcD7FaW8OvSS8SHaiEkthohElPBY1NOslVkR8owzNQjLZ8T6Q"



class StdOutListener(StreamListener):


    def __init__(self, api=None):
        super(StdOutListener, self).__init__()
        self.num_of_tweets = 0
        open('tweets.txt', 'w').close()
        
    def on_connect(self):
        print("You're connected to the streaming server.")
        print("\n")   
    
    def on_data(self, data):
        client = MongoClient()
        db = client.tweetdb
        datajson = json.loads(data)
        
        x =  datajson['text']
        munged={}
        munged['id'] =  datajson['id']
        munged['created_at'] = datajson['created_at']
        munged['text'] = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",x).split())
        munged['location'] = datajson['user']['location']
        munged['time_zone'] = datajson['user']['time_zone']
        
        
        json_data = json.dumps(munged)
        db.iphonetweets.insert_one(json.loads(json_data))
        
        print json_data
        with open('tweets.txt', 'a') as tf:
            tf.write(json_data)
        
        self.num_of_tweets = self.num_of_tweets + 1
        if self.num_of_tweets <10:
            return True
        else:
            tf.close()
            return False

    def on_error(self, status):
        print status

    

if __name__ == '__main__':


    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['iphone'],languages=['en'])
    
    
    sum = 0.0
    alist = []
    positive = []
    negative = []
    neutral = []
    client = MongoClient()
    db = client.tweetdb
    for iphonetweet in db.iphonetweets.find():
        tweet = TextBlob(iphonetweet["text"])
        alist.append(tweet.sentiment.polarity)
        print tweet.sentiment.polarity
        if tweet.sentiment.polarity>0:
            positive.append(tweet.sentiment.polarity)
        elif  tweet.sentiment.polarity<0:  
            negative.append(tweet.sentiment.polarity)
        else:
            neutral.append(tweet.sentiment.polarity)
        sum = sum + tweet.sentiment.polarity
        
    total = db.iphonetweets.count()  
      
    print "Mean:" , statistics.mean(alist)  
    print "Median:" , statistics.median(alist)
    print "Mode:" , statistics.mode(alist)
    print "Standard deviation" , statistics.stdev(alist, xbar=None)
    
    people = ('Positive', 'Negative', 'Neutral')
    y_pos = np.arange(len(people))
    sizes=[len(positive)/float(total), len(negative)/float(total), len(neutral)/float(total)]
   
plt.barh(y_pos, sizes, alpha=0.4, align='center')
plt.yticks(y_pos, people)
plt.xlabel('Sizes')
plt.title('Product Sentiment Analysis')
plt.show()
#      y_pos = np.arange(len(people))
     
#     colors = {'blue', 'white', 'yellow'}
#     sizes={len(positive)/float(total), len(negative)/float(total), len(neutral)/float(total)}
#     y_pos = np.arange(len(people))
#     print sizes
#     error = np.random.rand(len(people))
#     plt.barh(y_pos, sizes, xerr=error, align='center', alpha=0.4)
#     plt.yticks(y_pos, people)
#     plt.xlabel('sizes')
#     plt.title('Product Sentiment Analysis')
# 
# plt.show()
#     
    
    
    
#     labels = ['Positive', 'Negative', 'Neutral']
#     colors = ['blue', 'white', 'yellow']
#      
#     sizes=[len(positive)/float(total), len(negative)/float(total), len(neutral)/float(total)]
#     print sizes
#     plt.pie(sizes, labels = labels, colors=colors, autopct='%1.1f%%', shadow=True)
#     plt.axis('equal')
#     plt.show()