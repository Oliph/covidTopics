"""
Wrapper around the REST Stream Search
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

import requests

from requests import ConnectionError

import tweepy

from pymongo import errors as PyError, MongoClient

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
duplicate_tweets = 0
inserted_tweets = 0


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
    global duplicate_tweets, inserted_tweets
    try:
        collection.insert_one(tweet)
        inserted_tweets += 1

    except PyError.DuplicateKeyError:
        print("original tweet id: {}".format(tweet['id']))
        db_tweet_find = collection.find_one({'id': tweet['id']}, {'id': True, '_id': False})
        print("db tweet id: {}".format(db_tweet_find))
        
        duplicate_tweets += 1
    except TypeError:
        logger.error("Error in insert_record, not a dict to insert: {}".format(tweet))


def search_tweets(
    collection,
    api,
    list_terms,
    max_id=None,
    until_period=None,
    since_id=None,
    debug=False,
):
    """
    Run the REST API Search to get the tweets
    :params:
        collection mongodb.collection(): Where to insert tweets
        api twitter.api(): api connection to twitter rest
        list_terms list(): terms to search Default operator OR
        until_period datetime() the day to return before
        max_id int(): tweet id of the last recorded tweet
        since_id int(): return tweet that are more recent than that one
    """
    global total_tweets, duplicate_tweets, inserted_tweets
    for tweets in api.search_tweets(
        search_terms=list_terms, max_id=max_id, until=until_period
    ):
        for t in tweets.response["statuses"]:
            total_tweets += 1
            if debug is True:
                print(t)
            else:
                insert_tweet(collection, t)

            if total_tweets % 1000 == 0:
                logger.info("Collected: {}".format(total_tweets))
                logger.info("Duplicated: {}".format(duplicate_tweets))
                logger.info("Inserted: {}".format(inserted_tweets))
                logger.info(
                    "Final reports\n\tCollected: {}\n\tInserted: {}\n\tDuplicated:{}".format(
                        total_tweets, inserted_tweets, duplicate_tweets
                    )
                )


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

    # initialize stream
    list_terms = [
        "lancet",
        "#lancet",
        "The lancet",
        "#hydroxychloroquine",
        "hydroxychloroquine",
        "chloroquine",
        "#chloroquine",
    ]
    logger.info("Run the Search API")
    search_tweets(collection_tweet, rest_api, list_terms)
