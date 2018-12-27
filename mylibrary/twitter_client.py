# 27 Dec 2018 | Twitter Streaming Client + Produce to Kafka 

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from mylibrary.kafka_producer import KafkaProducer
import json
import logging

log = logging.getLogger(__name__)


class TwitterListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, kafka_topic):
        self.kafka_topic = kafka_topic

    def on_data(self, data):
        js = json.loads(data)
        #print(js)
        kp = KafkaProducer()
        print(self.kafka_topic)
        kp.produce_msg(self.kafka_topic, data)
        return True

    def on_error(self, status):
        if status == 420:
            #returning False in on_data disconnects the stream
            return False
        else:
            print(status)
            return True


class TwitterClient(object):

    def load_credentials(self):
        """Load Credentials"""
        with open('twitter.key', 'r') as twitter_key_file:
            twitter_key = json.load(twitter_key_file)
        return twitter_key

    def authenticate(self, twitter_key):
        """Authenticate"""
        twitter_auth = OAuthHandler(twitter_key[0]['CONSUMER_KEY'], twitter_key[0]['CONSUMER_SECRET'])
        twitter_auth.set_access_token(twitter_key[0]['ACCESS_TOKEN_KEY'], twitter_key[0]['ACCESS_TOKEN_SECRET'])
        return twitter_auth

    def get_tweets_stream(self, kafka_topic, hashtag):
        """Get Tweets Streaming"""
        twitter_auth = self.authenticate(self.load_credentials())
        twitter_listener = TwitterListener(kafka_topic)

        twitter_stream = Stream(twitter_auth, twitter_listener)
        twitter_stream.filter(track=[hashtag])
