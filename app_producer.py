# 12 Dec 2018 | TwitMine

"""Twitter Data Miner
Application that:
"""

import logging
from time import gmtime, strftime
from tweepy import Stream
from mylibrary.twitter_client import TwitterClient

log = logging.getLogger(__name__)

kafka_topic = 'twitter_trend'
hashtag = '#'

if __name__ == '__main__':

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <START>")

    # Logger | Initialize
    fmt_string = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    fmtr = logging.Formatter(fmt=fmt_string)
    sh = logging.StreamHandler()
    sh.setFormatter(fmtr)
    my_lib_logger = logging.getLogger("mylibrary")
    my_lib_logger.addHandler(sh)

    # Logger | Set Level
    my_lib_logger.setLevel("INFO")

    # Instantiate Objects
    TwClient = TwitterClient()
    TwClient.get_tweets_stream(kafka_topic, hashtag)

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")
