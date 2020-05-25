#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Olivier PHILIPPE"

"""
Connect to the REST API to collect missing tweet s if Stream failed
Parse the db and check where there is missing information between two dates
When it is the case, get the oldest and the newest tweets that bound the period
then run a search in between these boundaries
and update the db with them
"""
import os
import sys
import time  # datetime.now().strftime('%Y-%m-%d %H:%M:%S')
import asyncio
import datetime
import urllib.parse as urllib

from twitterAccess.RESTApi import TwitterRESTAPI

import pymongo
import requests
import pandas as pd

from pymongo import errors as PyError, MongoClient
from requests import ConnectionError

import tweepy

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


def connect_db():
    host = os.environ["DB_HOST"]
    port = int(os.environ["DB_MONGO_PORT"])
    database = os.environ["DB_MONGO_DATABASE"]
    user = os.environ["DB_MONGO_USER"]
    passw = os.environ["DB_MONGO_PASS"]
    client = MongoClient(host, port, username=user, password=passw)
    # logger.info("server_info():", client.server_info())
    logger.info("server_info(): {}".format(client.server_info()))
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
        # logger.info("Error in insert_record, not a dict to insert: {}".format(tweet))


def find_last_tweet_from_stream(collection):
    """
    Find the last tweet inserted in the db and return the tweet id to
    be passed into the search API
    """
    last_tweet = collection.find_one(
        {},
        {"id": True, "created_at": True, "_id": False},
        sort=[("_id", pymongo.ASCENDING)],
    )
    logger.info(
        "Last recorded tweet before crash: id: {} - date: {}".format(
            last_tweet["id"], last_tweet["created_at"]
        )
    )
    return last_tweet["id"]


def search_missing_period(
    collection, api, list_terms, max_id=None, until_period=None, since_id=None
):
    """
    Run the REST API Search to get the tweets missing since
    the crash
    :params:
        collection mongodb.collection(): Where to insert tweets
        api twitter.api(): api connection to twitter rest
        until_period datetime() the day to return before
        max_id int(): tweet id of the last recorded tweet
        since_id int(): return tweet that are more recent than that one
    """
    for tweets in api.search_tweets(
        search_terms=list_terms, max_id=max_id, since_id=since_id
    ):
        print(tweets.__dict__)
        # for t in tweets.response["statuses"]:
        # print(t)
        # insert_tweet(collection, t)


def parse_count_per_hours(collection):
    date_pipeline = [
        {"$project": {"date": {"$dateFromString": {"dateString": "$created_at"}}}},
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$date"},
                    "month": {"$month": "$date"},
                    "day": {"$dayOfMonth": "$date"},
                    "hour": {"$hour": "$date"},
                },
                "count": {"$sum": 1},
            }
        },
        {
            "$project": {
                "date": {
                    "$dateFromParts": {
                        "year": "$_id.year",
                        "month": "$_id.month",
                        "day": "$_id.day",
                        "hour": "$_id.hour",
                    }
                },
                "count": "$count",
                "_id": 0,
            }
        },
        {"$sort": {"date": 1}},
    ]
    return collection.aggregate(date_pipeline)


def create_reference_time(min_date, max_date, step_freq="h"):
    # return pd.DatetimeIndex(start=min_date, end=max_date, freq=step_freq)
    return pd.date_range(min_date, max_date, freq=step_freq)


def get_gaps(date, ref):
    return ref[~ref.isin(date)]


def add_hours_lower_threshold(counts, threshold):
    return [x for x in counts if counts[x] < threshold]


def create_time_range(gaps):
    time_range = list()
    previous_hour = gaps[0]
    start_gap = previous_hour
    for i in gaps[1:]:
        diff = i - previous_hour
        if diff.total_seconds() / 3600 == 1:
            start_gap = i
        else:
            time_range.append((start_gap, i))
        previous_hour = i
    return time_range


def find_tweet_boundaries(collection, min_date, max_date):
    date_pipeline = [
        {
            "$project": {
                "date": {"$dateFromString": {"dateString": "$created_at"}},
                "id": 1,
                "_id": 0,
            }
        },
        {"$match": {"date": {"$gte": min_date, "$lte": max_date}}},
        {"$sort": {"date": 1}},
        {
            "$group": {
                "_id": None,
                "first": {"$first": "$$ROOT"},
                "last": {"$last": "$$ROOT"},
            }
        },
    ]
    results = list(collection.aggregate(date_pipeline))[0]
    return results["first"], results["last"]


def getting_missing_gap(collection):
    """
    Parse the db and return the gap in period with tweets ids as boundaries
    """
    count_per_hours = pd.DataFrame([i for i in parse_count_per_hours(collection)])
    min_date = min(count_per_hours["date"])
    max_date = max(count_per_hours["date"])
    ref_time = create_reference_time(min_date, max_date)
    gaps = get_gaps(count_per_hours["date"], ref_time)
    gaps_range = create_time_range(gaps)
    for x in gaps_range:
        first, last = find_tweet_boundaries(collection, x[0], x[1])
        diff_days = datetime.datetime.now() - last["date"]

        if diff_days.days >= 7:

            logger.info(
                "Skipping it, too old for accessing it: {} days".format(diff_days.days)
            )
        else:
            logger.info(
                "Searching for the gap between: {} and {}".format(
                    first["date"].date(), last["date"].date()
                )
            )
            yield first["id"], last["id"]


if __name__ == "__main__":
    # ### LOAD ENV ################################################
    from dotenv import load_dotenv

    load_dotenv()

    # logger.info("Starting the process")
    mongodb = connect_db()

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
    until_period = str(
        datetime.datetime.date(datetime.datetime.now() - datetime.timedelta(days=1))
    )
    for first_id, last_id in getting_missing_gap(collection_tweet):

        search_missing_period(
            collection=collection_tweet,
            api=api,
            list_terms=list_terms,
            max_id=first_id,
            since_id=last_id,
        )
