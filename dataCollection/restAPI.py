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
import pymongo

from pymongo import errors as PyError, MongoClient

# Logging
import logging

logging.basicConfig(format="%(asctime)s::%(message)s", datefmt="%d-%b-%y %H:%M:%S")
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


def insert_tweet(collection, tweet):
    """ """
    try:
        collection.insert_one(tweet)
    except PyError.DuplicateKeyError:
        pass
    except TypeError:
        logger.info("Error in insert_record, not a dict to insert: {}".format(tweet))


def find_last_tweet_from_stream(collection):
    """
    Find the last tweet inserted in the db and return the tweet id to
    be passed into the search API
    """
    last_tweet = collection.find_one(
        {},
        {"id": True, "created_at": True, "_id": False},
        sort=[("_id", pymongo.DESCENDING)],
    )
    logger.info(
        "Last recorded tweet before crash: id: {} - date: {}".format(
            last_tweet["id"], last_tweet["created_at"]
        )
    )
    return last_tweet["id"]


def search_missing_period(collection, api, list_terms, last_tweet_id=None, until_period=None):
    """
    Run the REST API Search to get the tweets missing since
    the crash
    :params:
        collection mongodb.collection(): Where to insert tweets
        api twitter.api(): api connection to twitter rest
        last_tweet_id int(): tweet id of the last recorded tweet
    """
    for tweets in api.search_tweets(
        list_terms, since_id=last_tweet_id, until=until_period
    ):
        for t in tweets.response["statuses"]:
            insert_tweet(collection, t)


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
    api = TwitterRESTAPI(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret,
        wait_on_pause=True,
    )
    with open('last_tweet', 'r') as f:
        last_tweet_id = int(f.readlines()[0])
    until_period = str(datetime.date(datetime.now()))
    logger.info('Search tweets from {} until the last_tweet: {}'.format(until_period, last_tweet_id))
    search_missing_period(
        collection=collection_tweet, api=api, list_terms=list_terms, last_tweet_id=last_tweet_id, until_period=until_period
    )
    # for tweets in twitter_api.search_tweets(list_terms, since_id=last_tweet_id):
    #    print(tweets.response)
