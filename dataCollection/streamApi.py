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
from twitterAccess.RESTApi import TwitterRESTAPI

import tweepy
import requests

from pymongo import errors as PyError, MongoClient
from requests import ConnectionError

# Logging
import logging

logger_level = "DEBUG"
stream_level = "INFO"
file_level = "ERROR"

logger = logging.getLogger(__name__)
logger_set_level = getattr(logging, "INFO")
logger.setLevel(logger_set_level)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(name)s :: %(message)s")

stream_handler = logging.StreamHandler()
stream_set_level = getattr(logging, stream_level)
stream_handler.setLevel(stream_set_level)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

total_tweets = 0


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
        global total_tweets
        total_tweets += 1
        self.insert_tweet(status._json)
        # logger.info("Inserted tweet: {}".format(self.total_tweets))
        # self.queue.put(status)
        if total_tweets % 1000 == 0:
            logger.info("Collected: {}".format(total_tweets))

    def on_error(self, status_code):
        logger.error("Encountered streaming error: {}".format(status_code))
        return True

    def filter(self, keywords=None, to_async=True):
        streamer = self.__streamer__()
        try:
            logger.info("Starting steam")
            streamer.filter(track=keywords, is_async=to_async)
        except Exception as ex:
            logger.error("Stream stoppped. Error: ".format(e))
            # logger.error("Reconnecting to twitter stream")
            # self.filter(keywords=keywords, is_async=to_async)


def get_stream_data(streamAPI, queue, search_terms=["hate speech"]):
    stream.filter(track=search_terms)


if __name__ == "__main__":

    # ### LOAD ENV ################################################
    logger.info("Run the software")

    from pathlib import Path

    from dotenv import load_dotenv

    env_path = os.path.join(Path().resolve().parent, ".env")
    load_dotenv(dotenv_path=env_path)

    logger.info("Connect to db")
    mongodb = connect_db()
    logger.info(mongodb)

    collection_tweet = mongodb["tweets-lancet"]
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
    logger.info("Connect to Twitter API")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream_api = tweepy.API(auth)

    # initialize stream
    logger.info("Init the streamlistener")
    streamListener = StreamListener(collection_tweet)
    stream = tweepy.Stream(
        auth=stream_api.auth, listener=streamListener, tweet_mode="extended"
    )
    list_terms = [
        "lancet",
        "#lancet",
        "The lancet",
        "#hydroxychloroquine",
        "hydroxychloroquine",
        "chloroquine",
        "#chloroquine",
    ]
    logger.info("Get the last inserted tweet")
    z = 0
    while True:
        try:
            logger.info("Run the Stream for {} times".format(z))
            stream.filter(track=list_terms, is_async=False)
            until_period = str(datetime.date(datetime.now()))
        except Exception as e:
            logger.error(e)
            z += 1
            logger.error("Crashed for the {} times".format(z))
            time.sleep(30)
