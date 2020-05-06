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

import tweepy
import requests

from pymongo import errors as PyError, MongoClient
from requests import ConnectionError

# Logging
from logger import logger as logger_perso

logger = logger_perso(name="twitterStreamApi", stream_level="INFO", file_level="ERROR")


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
            print("Collected: {}".format(self.total_tweets))

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        return True

    def filter(self, keywords=None, to_async=True):
        streamer = self.__streamer__()
        try:
            print("[STREAM] Started steam")
            streamer.filter(track=keywords, is_async=is_async)
        except Exception as ex:
            print("[STREAM] Stream stopped! Reconnecting to twitter stream")
            print(ex.message, ex.args)
            self.filter(keywords=keywords, is_async=is_async)


def get_stream_data(streamAPI, queue, search_terms=["hate speech"]):
    stream.filter(track=search_terms)


if __name__ == "__main__":

    # ### LOAD ENV ################################################
    from dotenv import load_dotenv

    load_dotenv()

    mongodb = connect_db()

    logger.info("Starting the process")
    # logger.ERROR("Test")
    # logger.info("Working")
    # raise
    collection_tweet = mongodb["tweets"]
    # Create unique index
    ensure_unique_index(collection_tweet, "id")

    ### TWITTER CONNECTION

    consumer_key = os.environ["TWITTER_CONSUMER_KEY"]
    consumer_secret = os.environ["TWITTER_CONSUMER_SECRET"]
    access_token = os.environ["TWITTER_ACCESS_TOKEN"]
    access_token_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

    # complete authorization and initialize API endpoint
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # initialize stream
    streamListener = StreamListener(collection_tweet)
    stream = tweepy.Stream(
        auth=api.auth, listener=streamListener, tweet_mode="extended"
    )
    list_terms = ["desconfinament", "desescalda", "desconfinamiento", "desescalada"]
    stream.filter(track=list_terms, is_async=True)
