import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from cryptocmd import CmcScraper
from datetime import date
import psycopg2
import numpy as np
import psycopg2.extras as extras
from airflow.providers.postgres.hooks.postgres import PostgresHook 

import numpy as np
import datetime as dt
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import snscrape.modules.twitter as sntwitter
from textblob import TextBlob
nltk.download('vader_lexicon')

hook = PostgresHook(postgres_conn_id= "postgres_con")
conn = hook.get_conn()

def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


def get_crypto_trading_data():

    #init scraper
    url = "https://api.alternative.me/fng/?"
    
    querystring = {"limit":"2"}
    
    now = dt.datetime.now()
    now = now.strftime('%d-%m-%Y')
    yesterday = dt.datetime.now() - dt.timedelta(days = 1)
    yesterday = yesterday.strftime('%d-%m-%Y')
    scraper = CmcScraper('BTC', yesterday, now)

    #get trading dataframe
    price_df = scraper.get_dataframe()
    #rename column for loading to db
    price_df = price_df.rename({'Date':'date', 'Open':'open', 'High':'high', 'Low':'low', 'Close':'close', 'Volume':'volume', 'Market Cap': 'marketcap'},axis = 1)
    
    ###prepare sentiment data
    #get response
    response = requests.request("GET", url, params=querystring)

    response_json = response.json()
    timestamp = []
    fng_score = []

    for day in response_json["data"]:
        timestamp.append(int(day["timestamp"]))
        fng_score.append(int(day["value"]))

    #remove current sentiment data
    timestamp.pop(0)
    fng_score.pop(0)

    #create sentiment dict
    sentiment_dict = {
        "timestamp" : timestamp,
        "fng_score" : fng_score
    }

    #create sentiment_df
    sentiment_df = pd.DataFrame(sentiment_dict, columns= ["timestamp","fng_score"])

    #timestamp to date format
    sentiment_df["timestamp"] = pd.to_datetime(sentiment_df["timestamp"], unit='s')
    #rename timestamp column to date
    sentiment_df = sentiment_df.rename({'timestamp':'date'},axis=1)

    
    #update db
    execute_values(conn, sentiment_df, 'fng_index')
    execute_values(conn, price_df, 'trading_data')

    
def get_tweet_and_analysis_sentiment():
    #Creating list to append tweet data
    tweets_list = []

    now = dt.datetime.now()
    now = now.strftime('%Y-%m-%d')
    yesterday = dt.datetime.now() - dt.timedelta(days = 1)
    yesterday = yesterday.strftime('%Y-%m-%d')

    #Loop through the usernames:
    user_names = open('dags/Influential_People1.txt','r')

    for user in user_names:
        print ('scrawling for '+ user)
        # Using TwitterSearchScraper to scrape data and append tweets to list
        for i,tweet in enumerate(sntwitter.TwitterSearchScraper("from:"+ user + 'bitcoin' + ' since:' + yesterday + ' until:' + now).get_items()):
            tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.username])
        for i,tweet in enumerate(sntwitter.TwitterSearchScraper("from:"+ user + 'btc' + ' since:' + yesterday + ' until:' + now).get_items()):
            tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.username])


    # Creating a dataframe from the tweets list above 
    df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])
    df['Datetime'] = pd.to_datetime(df['Datetime'],unit='ms')
    df['Datetime'] = df['Datetime'].apply(lambda a: dt.datetime.strftime(a,"%d-%m-%Y %H:%M:%S"))
    df['Datetime'] = pd.to_datetime(df['Datetime'])          
    df['Tweet Id'] = ('"'+ df['Tweet Id'].astype(str) + '"')      

    # Create a function to clean the tweets
    def cleanTxt(text):
        text = re.sub('@[A-Za-z0–9]+', '', text) #Removing @mentions
        text = re.sub('#', '', text) # Removing '#' hash tag
        text = re.sub('RT[\s]+', '', text) # Removing RT
        text = re.sub('https?:\/\/\S+', '', text) # Removing hyperlink
        return text

    df["Text"] = df["Text"].apply(cleanTxt)


    #Sentiment Analysis
    #Iterating over the tweets in the dataframe
    def apply_analysis(tweet):
        return SentimentIntensityAnalyzer().polarity_scores(tweet)

    df[['neg','neu','pos','compound']] = df['Text'].apply(apply_analysis).apply(pd.Series)

    def getSubjectivity(twt):
        return TextBlob(twt).sentiment.subjectivity
    def getPolarity(twt):
        return TextBlob(twt).sentiment.polarity

        
    df['Subjectivity']=df['Text'].apply(getSubjectivity)
    df['Polarity']=df['Text'].apply(getPolarity)    

    # Merging with NLP results
    df.rename(columns = {'Datetime' : 'date', 'compound' : 'nlp_compound', 'Subjectivity' : 'nlp_subjectivity', 'Polarity' : 'nlp_polarity'}, inplace = True)
    df['date'] = pd.to_datetime(df['date']).dt.date

    df = df.groupby('date', dropna = True).mean()[['nlp_compound', 'nlp_subjectivity', 'nlp_polarity']]
    df.reset_index(inplace=True)

    #update db
    execute_values(conn, df, 'twitter_sentiment')