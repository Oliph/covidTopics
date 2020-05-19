import os
import gzip

from pathlib import Path
from twitterAccess.RESTApi import TwitterRESTAPI

from dotenv import load_dotenv
from pymongo import errors as PyError, MongoClient

env_path = os.path.join(Path().resolve().parent, ".env")
load_dotenv(dotenv_path=env_path)


def connect_db(host=None, port=None, database=None, user=None, passw=None):
    if host is None:
        host = os.environ["DB_HOST"]
    if port is None:
        port = int(os.environ["DB_MONGO_PORT"])
    if database is None:
        database = os.environ["DB_MONGO_DATABASE"]
    if user is None:
        user = os.environ["DB_MONGO_USER"]
    if passw is None:
        passw = os.environ["DB_MONGO_PASS"]
    client = MongoClient(host, port, username=user, password=passw)
    return client[database]


def insert_tweet(collection_tweet, tweet):
    """ """
    try:
        collection_tweet.insert_one(tweet)
    # The collection is supposed to have a unique on id_ so error raised
    # if tweet already present
    except PyError.DuplicateKeyError:
        dup = collection_tweet.find_one({"id": tweet["id"]})
        try:
            dup["user"]["id"]
        except KeyError:
            collection_tweet.delete_one({"id": tweet["id"]})
            collection_tweet.insert_one(tweet)
    except TypeError:
        print("Error in insert_record, not a dict to insert: {}".format(tweet))


def windows(iterable, size, step=1):
    it = iter(iterable)
    cur = list()
    while True:
        try:
            while len(cur) < size:
                cur.append(next(it))
            yield list(cur)
            for _ in range(step):
                if cur:
                    cur.pop(0)
                else:
                    next(it)
        except StopIteration:
            return


def check_tweet(tweet, list_terms):
    """
    """
    list_terms.append("coro")
    # print(tweet["text"].lower().split(" "))
    # for i in list_terms:
    if any(tweet["text"].lower().split(" ")) in list_terms:
        print(tweet["text"].lower().split(" "))
        return True


def get_missing_tweets(api, collection, list_ids, file_parsed_ids):
    n = 0
    m = 0
    nbr_call = 0
    for list_100 in windows(list_ids, 100, 1):
        tweet = api.tweet_look_up(list_100)
        if tweet.status != 200:
            print(tweet.response)
        for t in tweet.response:
            print(t)
            # insert_tweet(collection, t)
            n += 1
        if n % 100 == 0:
            print("Done {} calls".format(n))
        with open(file_parsed_ids, "a") as f:
            f.write("\n".join([str(i) for i in list_100]))
            f.close()
    # print("Collected {} tweets".format(m + n))
    # print("Got {} tweets matching query".format(n))


def connect_API():
    # Load the keys for twitter API
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
    return twitter_api


def check_ids_to_db():
    pass


def get_ids_to_parse(data_directory, parsed_ids):
    for path in Path(data_directory).rglob("*_clean-dataset.tsv.gz"):
        with gzip.open(path, "r") as f:
            for l in f:
                try:
                    tweet_id = int(l.decode().strip().split("\t")[0])
                    if tweet_id not in parsed_ids:
                        yield tweet_id
                    else:
                        parsed_ids.remove(tweet_id)
                except ValueError:
                    pass


def get_parsed_ids(file_parsed_ids):
    to_return = list()
    try:
        with open(file_parsed_ids, "r") as f:
            for l in f:
                to_return.append(int(l[:-1]))
    except FileNotFoundError:
        pass
    return to_return


def main():
    twitter_api = connect_API()
    mongodb = connect_db(database="covidStream")
    collection_tweet = mongodb["tweets_test"]
    data_directory = "../../covid19_twitter/dailies"
    file_parsed_ids = "../data/parsed_ids.csv"
    list_terms = ["desconfinament", "desescalda", "desconfinamiento", "desescalada"]
    parsed_ids = get_parsed_ids(file_parsed_ids)
    ids_to_parse = get_ids_to_parse(data_directory, parsed_ids)
    get_missing_tweets(twitter_api, collection_tweet, ids_to_parse, file_parsed_ids)


if __name__ == "__main__":
    main()
