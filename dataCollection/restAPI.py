#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Olivier PHILIPPE"

"""
Connect to the REST API to collect missing tweet s if Stream failed
"""
import os
import sys
import time  # datetime.now().strftime('%Y-%m-%d %H:%M:%S')
import asyncio
import urllib.parse as urllib

from datetime import datetime
from twitterAccess.RESTApi import TwitterRESTAPI

import requests

from requests import ConnectionError

import tweepy

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
    list_terms = ["desconfinament", "desescalda", "desconfinamiento", "desescalada"]
    # list_terms = ["covid"]

    consumer_key = os.environ["TWITTER_CONSUMER_KEY"]
    consumer_secret = os.environ["TWITTER_CONSUMER_SECRET"]
    access_token = os.environ["TWITTER_ACCESS_TOKEN"]
    access_token_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]
    twitter_api = TwitterRESTAPI(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret,
        wait_on_pause=True,
    )
    for tweets in twitter_api.search_tweets(list_terms):
        print(tweets.response)
