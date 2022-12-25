import os
import json
import time as t
import numpy as np
import glob
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
from textblob import TextBlob
import csv
import plotly.express as px

def collectTweetsEveryHour(keyTerm):

        timenow = round(t.time())
        timesub5min = timenow - 300
        timesub1hour = timenow - 3600
        
        os.system(
                'snscrape --jsonl --progress --since {} twitter-search "{} until:{}" > twitter{}{}.json'.format(timesub1hour, keyTerm, timenow, keyTerm, timenow))
        
        print("Twitter Cycle Complete")
        return timenow

        
def collectPostsEveryHourreddit(keyTerm):

        timenow = round(t.time())
        timesub5min = timenow - 300
        timesub1hour = timenow - 3600

        os.system(
                'snscrape --jsonl --progress reddit-search {} --after {} --before {} > reddit{}{}.json'.format(keyTerm, timesub1hour, timenow, keyTerm, timenow))
        print("Reddit Cycle Complete")
        return timenow


def format_polarity_twitter(type,keyTerm,date):
   
    files = glob.glob("{}{}.json".format(keyTerm,date))

    cols = [
        "content", "retweetCount"
    ]

    data_df = pd.DataFrame(columns=cols)

    idx = 0

    file = open("/home/liveshare/Desktop/fullscrape/{}{}{}.json".format(type,keyTerm,date), "r")

    data = file.readlines()
    for d in data:
        current = []
        current_data = json.loads(d.replace("\r",""))
        user_data = current_data['user']
        for col in cols:
            try:
                data_piece = current_data[col]
                current.append(data_piece)
            except:
                data_piece = user_data[col]
                current.append(data_piece)
        data_df.loc[idx] = current
        idx += 1
        
    vader_sentiment = SentimentIntensityAnalyzer()
    data_df["sentiment_vader"] = data_df['content'].apply(lambda s: vader_sentiment.polarity_scores(s)['compound'])
    data_df["sentiment_textblob"] = data_df['content'].apply(lambda s: TextBlob(s).sentiment.polarity)
    data_df["retweetCount"] = data_df["retweetCount"] + 1
    data_df["sumPolarity"] = data_df["sentiment_vader"] * data_df["retweetCount"]
    sumPolarity = data_df["sumPolarity"].sum()
    volume = data_df["retweetCount"].sum()
    print(volume)
    print(sumPolarity)
    data = [date, sumPolarity, volume]
    print(data)    

    data_df.to_csv("{}{}{}_polar.csv".format(type, keyTerm, date), index=False)    
    return data



def format_polarity_reddit(type,keyTerm,date):
   
    cols = [
           "_type"
    ]

    data_df = pd.DataFrame(columns=cols)

    file = open("/home/liveshare/Desktop/fullscrape/{}{}{}.json".format(type,keyTerm,date), "r")

    data = file.readlines()    
    count = 0
    
    for d in data:
        current= []
        current_data = json.loads(d)
       
        for col in cols:
                if current_data[col] == "snscrape.modules.reddit.Submission":
                       current.append(current_data["selftext"])
                else:
                        current.append(current_data["body"])
               
                        
        data_df.loc[count] = current
        count += 1
    
    
    for index, row in data_df.iterrows():
            data = row["_type"]
            if data == None:
                row["_type"] = "nothing"

      
    vader_sentiment = SentimentIntensityAnalyzer()
    data_df["sentiment_vader"] = data_df['_type'].apply(lambda s: vader_sentiment.polarity_scores(s)['compound'])
    data_df["sentiment_textblob"] = data_df['_type'].apply(lambda s: TextBlob(s).sentiment.polarity)
        
    data_df.to_csv("{}{}{}_polar.csv".format(type, keyTerm, date), index=False)
          

def tweetPlotData(type,keyTerm,date):

        df = pd.read_csv("/home/liveshare/Desktop/fullscrape/{}{}{}_polar.csv".format(type,keyTerm,date))
        print(df.head())
        pd.to_datetime(df['date'])
        df = df.loc[(df != 0).any(axis=1)]
        df = df[(df[['sentiment_vader','sentiment_textblob']] != 0).all(axis=1)]
        df.plot(kind = 'scatter',
                x = 'date',
                y = 'sentiment_textblob',
                color = 'red')
        plt.show()
        
        
def redditPlotData(type,keyTerm,date):

        df = pd.read_csv("/home/liveshare/Desktop/fullscrape/{}{}{}_polar.csv".format(type,keyTerm,date), error_bad_lines=False, engine='python')
        print(df.head())
        pd.to_datetime(df['created'])
        df = df.loc[(df != 0).any(axis=1)]
        df = df[(df[['sentiment_vader','sentiment_textblob']] != 0).all(axis=1)]
        df.plot(kind = 'scatter',
                x = 'created',
                y = 'sentiment_textblob',
                color = 'red')
        plt.show()

def write_csv(data, term):
    with open("{}.csv".format(term), 'a') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(data)
        
        
def plotScores(term):
        df = pd.read_csv("{}.csv".format(term))
        print(df.head())
        # pd.to_datetime(df['date'])
        df.plot(kind = 'scatter',
                x = 'DateTime',
                y = 'Score',
                color = 'red')
        plt.show(block=False)
        fig = px.line(df, x = 'DateTime', y = 'Score', title=term)
        fig.show()
        # fig.write_html("rando.html")


        
        
        
def comboRun(keyTerm):
        cols = ["DateTime", "Score", "Volume"]
        write_csv(cols, keyTerm)
        while(True):    
                startTime = round(t.time()) 
                fiveMins = 3600               
                twitterFileTime = collectTweetsEveryHour(keyTerm)
                #redditFileTime = collectPostsEveryHourreddit(keyTerm)
                endTime = round(t.time())
                sleeptime = (fiveMins - (endTime - startTime))
                data = format_polarity_twitter("twitter", keyTerm, twitterFileTime)
                write_csv(data, keyTerm)
                #plotScores(keyTerm)
                # format_polarity_reddit("reddit",keyTerm,redditFileTime)
                # tweetPlotData("twitter", keyTerm, twitterFileTime) 
                # redditPlotData("reddit", keyTerm, redditFileTime)
                print(sleeptime)
                t.sleep(sleeptime)


comboRun("ethereum")

 