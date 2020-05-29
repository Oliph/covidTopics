# -*- coding: utf-8 -*-
# __author__ = "Olivier PHILIPPE"

"""
Connect to the REST API to collect earlier tweets than the ones started with the stream if the oldest tweet recorded
is less than 7 days old
"""
import os
import sys
import time  # datetime.now().strftime('%Y-%m-%d %H:%M:%S')
import asyncio
import argparse
import datetime
import importlib
import urllib.parse as urllib

from twitterAccess.RESTApi import TwitterRESTAPI

import requests

from requests import ConnectionError
from dateutil.parser import parse

# import config
import tweepy
import pymongo
import pandas as pd

from pymongo import errors as PyError, MongoClient
from searchTweets import search_tweets

# Logging
import logging

logger_level = "DEBUG"
stream_level = "INFO"
file_level = "ERROR"

logger = logging.getLogger(__name__)
logger_set_level = getattr(logging, "INFO")
logger.setLevel(logger_set_level)
formatter = logging.Formatter(
    "%(asctime)s :: %(levelname)s :: %(name)s :: %(lineno)s :: %(message)s"
)

stream_handler = logging.StreamHandler()
stream_set_level = getattr(logging, stream_level)
stream_handler.setLevel(stream_set_level)
stream_handler.setFormatter(formatter)
logger.handlers[:] = [stream_handler]


def connect_db():
    host = os.environ["DB_HOST"]
    port = int(os.environ["DB_MONGO_PORT"])
    database = os.environ["DB_MONGO_DATABASE"]
    user = os.environ["DB_MONGO_USER"]
    passw = os.environ["DB_MONGO_PASS"]
    client = MongoClient(host, port, username=user, password=passw)
    logger.info("server_info(): {}".format(client.server_info()))
    return client[database]


def ensure_unique_index(collection, key):

    collection.create_index(key, unique=True)


def find_earliest_tweet(collection):
    """
    Return the id and the date of the earliest tweet from the dabatase
    Can be used to download tweets prior to that date if it is not later
    than 7 days from the day it is run
    """

    date_pipeline = [
        {
            "$project": {
                "date": {"$dateFromString": {"dateString": "$created_at"}},
                "id": True,
            }
        },
        {"$sort": {"date": 1}},
        {"$group": {"_id": None, "first": {"$first": "$$ROOT"}}},
    ]

    first_tweet_id = list(collection.aggregate(date_pipeline))[0]
    full_tweet = collection.find_one({"_id": first_tweet_id["first"]["_id"]})
    # replace the created_time entry with the datetime object from here
    full_tweet["created_at"] = first_tweet_id["first"]["date"]
    return full_tweet


if __name__ == "__main__":
    # ### LOAD ENV ################################################
    from dotenv import load_dotenv

    load_dotenv()

    logger.info("Starting the process")

    # Parsing the config file name
    parser = argparse.ArgumentParser(
        description="Download earliest tweets than from the starting date of the stream data collection if the day is less than 7 days"
    )
    parser.add_argument("-c", "--config", type=str, default="config")
    args = parser.parse_args()
    config = importlib.import_module(args.config)

    mongodb = connect_db()
    col_tweet_name = config.config.collection_tweet
    logger.info("Tweets are stored into: {}".format(col_tweet_name))
    collection_tweet = mongodb[col_tweet_name]
    # Create unique index
    ensure_unique_index(collection_tweet, "id")

    ### TWITTER CONNECTION
    list_terms = config.config.list_terms
    logger.info("Getting the following terms: {}".format(list_terms))

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
    until_period = str(
        datetime.datetime.date(datetime.datetime.now() - datetime.timedelta(days=1))
    )
    try:
        earliest_tweet = find_earliest_tweet(collection_tweet)
    except Exception as e:
        earliest_tweet = dict()
        earliest_tweet['created_at']  = datetime.datetime.now()

    diff_days = datetime.datetime.now() - earliest_tweet["created_at"]
    # print(earliest_tweet["_id"])
    # print(earliest_tweet["created_at"])
    if diff_days.days < 7:
        until_date = earliest_tweet["created_at"].strftime("%Y-%m-%d")
        search_tweets(
            collection_tweet,
            api,
            list_terms,
            until_period=until_date,
            # max_id=int(earliest_tweet["id"]),
            debug=False,
        )
