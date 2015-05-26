from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pymongo import MongoClient
from textblob import TextBlob
import statistics
import json
import re
from collections import Counter
import sys
import markup
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
from pytagcloud import create_tag_image, make_tags
from pytagcloud.lang.counter import get_tag_counts
import operator
import obo
import webbrowser

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
        if self.num_of_tweets <5:
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
    stream.filter(track=['iphone,iPhone'],languages=['en'])
    sum = 0.0
    sum1 = 0.0
    layout = 3
    background_color = (255, 255, 255)
    max_word_size = 80
    max_words = 180
    width = 1280
    height = 800
    my_text=''
    f = open('stopwords.txt', 'rb')
    stop_words = [line.strip() for line in f]
    tweets_words = ['iphone', 'iPhone', 'amp']
    alist = []
    positive = []
    negative = []
    neutral = []
    client = MongoClient()
    db = client.tweetdb
    for iphonetweet in db.iphonetweets.find():
        tweet = TextBlob(iphonetweet["text"])
        sum1 = sum1 + tweet.word_counts['screen']
        if tweet.sentiment.subjectivity != 0.0:
            important_words = iphonetweet['text'].lower()
            my_text += important_words
            alist.append(tweet.sentiment.polarity)
        if tweet.sentiment.polarity>0:
            positive.append(tweet.sentiment.polarity)
        elif  tweet.sentiment.polarity<0: 
            negative.append(tweet.sentiment.polarity)
        else:
            neutral.append(tweet.sentiment.polarity)
    total= len(alist)
   
    words = my_text.split()
    counts = Counter(words)
    for word in stop_words:
        del counts[word]
    for word in tweets_words:
        del counts[word]
        
    print counts
    final = counts.most_common(max_words)
    max_count = max(final, key=operator.itemgetter(1))[1]
    final = [(name, count / float(max_count))for name, count in final]
    tags = make_tags(final, maxsize=max_word_size)
    create_tag_image(tags, 'cloud_large.png', size=(width, height), layout=layout, fontname='Lobster', background = background_color)
    
    
    
    people = ('Positive', 'Negative', 'Neutral')
    y_pos = np.arange(len(people))
    sizes=[len(positive)/float(total), len(negative)/float(total), len(neutral)/float(total)]
  
    plt.barh(y_pos, sizes, alpha=0.4, align='center')
    plt.yticks(y_pos, people)
    plt.title('Product Sentiment Analysis')
    savefig('barchart.png')
    
    plt.clf()
    labels = ['Positive', 'Negative', 'Neutral']
    colors = ['blue', 'green', 'yellow']
   
    sizes=[len(positive)/float(total), len(negative)/float(total), len(neutral)/float(total)]
    plt.pie(sizes, labels = labels, colors=colors, autopct='%1.1f%%', shadow=True)
    plt.axis('equal')
    savefig('piechart.png') 
   
        
   
    f = open('Output.html','w')

    message = """<html lang="en">
<head>
<title>Sentiment Analysis</title>
</head>
<body>
Twitter Sentiment Analysis
<br />
<h1>0.226129100117</h1>
<img src="barchart.png" alt="Fantastic!" width="400" height="400" border="2" />
<img src="piechart.png" alt="Fantastic!" width="500" height="400" border="2" />
<img src="cloud_large.png" alt="Fantastic!" width="400" height="400" border = "2"/>
<br />
<img src="1.png" alt="Fantastic!" width="30" height="30" />
<br />
<img src="2.png" alt="Fantastic!" width="30" height="30" />
<br />
<img src="3.png" alt="Fantastic!" width="30" height="30" />
<br />
<img src="4.png" alt="Fantastic!" width="30" height="30" />
<br />
<img src="5.png" alt="Fantastic!" width="30" height="30" />
<br />
<h2>409</h2>
<h2>27.0</h2>
This is the end.
</body>
</html>"""

    f.write(message)
    f.close()
    