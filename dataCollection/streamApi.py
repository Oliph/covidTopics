#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Olivier PHILIPPE"

"""
Wrapper around the Stream API from tweepy
"""
import os
import sys
import time  # datetime.now().strftime('%Y-%m-%d %H:%M:%S')
import asyncio
import urllib.parse as urllib

from datetime import datetime
from collections import deque
from multiprocessing import Process, Queue

import requests

from requests import ConnectionError

import tweepy

from twitterAccess.RESTApi import TwitterRESTAPI
import restAPI 

from pymongo import errors as PyError, MongoClient

# Logging
import logging

logging.basicConfig(format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")
logger = logging.getLogger(__name__)


def connect_db():
    host = os.environ["DB_HOST"]
    port = int(os.environ["DB_MONGO_PORT"])
    database = os.environ["DB_MONGO_DATABASE"]
    user = os.environ["DB_MONGO_USER"]
    passw = os.environ["DB_MONGO_PASS"]
    client = MongoClient(host, port, username=user, password=passw)
    logger.info("server_info():", client.server_info())
    return client[database]


def ensure_unique_index(collection, key):

    collection.create_index(key, unique=True)
    # collection.drop_index("id_1")


class StreamListener(tweepy.StreamListener):
    def __init__(self, collection_tweet, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection_tweet = collection_tweet
        self.total_tweets = 0

    def insert_tweet(self, tweet):
        """ """
        try:
            collection_tweet.insert_one(tweet)
        except PyError.DuplicateKeyError:
            pass
        except TypeError:
            logger.info(
                "Error in insert_record, not a dict to insert: {}".format(tweet)
            )

    def on_status(self, status):
        self.total_tweets += 1
        self.insert_tweet(status._json)
        # self.queue.put(status)
        if self.total_tweets % 1000 == 0:
            logger.info("Collected: {}".format(self.total_tweets))

    def on_error(self, status_code):
        logger.error("Encountered streaming error: {}".format(status_code))
        return True

    def filter(self, keywords=None, to_async=True):
        streamer = self.__streamer__()
        try:
            logger.info("Starting steam")
            streamer.filter(track=keywords, is_async=is_async)
        except Exception as ex:
            logger.error("Stream stoppped. Error: ".format(e))
            logger.error("Reconnecting to twitter stream")
            self.filter(keywords=keywords, is_async=is_async)


def get_stream_data(streamAPI, queue, search_terms=["hate speech"]):
    stream.filter(track=search_terms)


if __name__ == "__main__":

    # ### LOAD ENV ################################################
    from dotenv import load_dotenv

    load_dotenv()

    mongodb = connect_db()

    logger.info("Starting the process")
    collection_tweet = mongodb["tweets"]
    # Create unique index
    ensure_unique_index(collection_tweet, "id")

    ### TWITTER CONNECTION

    consumer_key = os.environ["TWITTER_CONSUMER_KEY"]
    consumer_secret = os.environ["TWITTER_CONSUMER_SECRET"]
    access_token = os.environ["TWITTER_ACCESS_TOKEN"]
    access_token_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]
    rest_api = TwitterRESTAPI(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret,
        wait_on_pause=True,
    )

    # complete authorization and initialize API endpoint
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream_api = tweepy.API(auth)

    # initialize stream
    streamListener = StreamListener(collection_tweet)
    stream = tweepy.Stream(
        auth=stream_api.auth, listener=streamListener, tweet_mode="extended"
    )
    list_terms = ["desconfinament", "desescalda", "desconfinamiento", "desescalada"]
    logger.info('Get the last inserted tweet')
    last_tweet = restAPI.find_last_tweet_from_stream(collection_tweet)
    #while True:
#    try:
    logger.info('Run the stream in async mode')
    stream.filter(track=list_terms, is_async=True)
    logger.info('Launch the REST API to get the missing tweets')
    restAPI.search_missing_period(collection_tweet, rest_api, list_terms, last_tweet)
#    except Exception as e:
#        logger.error(e)
#        logger.info('Get the last inserted tweet')
#        last_tweet = restAPI.find_last_tweet_from_stream(collection_tweet)
#        

