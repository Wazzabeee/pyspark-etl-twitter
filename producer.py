import tweepy
from kafka import KafkaProducer
import logging
import json
from decouple import config

"""API ACCESS KEYS"""
consumerKey = config('CONSUMERKEY')
consumerSecret = config('CONSUMERSECRET')
accessToken = config('ACCESSTOKEN')
accessTokenSecret = config('ACCESSTOKENSECRET')

logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'elon musk'
topic_name = 'twitter'


def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api


class TweetListener(tweepy.Stream):

    def on_data(self, raw_data):
        # logging.info(raw_data)

        tweet = json.loads(raw_data)
        tweet_text = ""
        # print(tweet.keys())

        if all(x in tweet.keys() for x in ['lang', 'created_at']) and tweet['lang'] == 'en':
            if 'retweeted_status' in tweet.keys():
                if 'quoted_status' in tweet['retweeted_status'].keys():
                    if 'extended_tweet' in tweet['retweeted_status']['quoted_status'].keys():
                        tweet_text = tweet['retweeted_status']['quoted_status']['extended_tweet']['full_text']
                elif 'extended_tweet' in tweet['retweeted_status'].keys():
                    tweet_text = tweet['retweeted_status']['extended_tweet']['full_text']
            elif tweet['truncated'] == 'true':
                tweet_text = tweet['extended_tweet']['full_text']

            else:
                tweet_text = tweet['text']

        if tweet_text:
            data = {
                'created_at': tweet['created_at'],
                'message': tweet_text.replace(',', '')
            }
            producer.send(topic_name, value=json.dumps(data).encode('utf-8'))

        # producer.send(topic_name, value=raw_data)
        return True

    @staticmethod
    def on_error(status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])


if __name__ == '__main__':
    twitter_stream = TweetListener(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    twitter_stream.start_streaming_tweets(search_term)
