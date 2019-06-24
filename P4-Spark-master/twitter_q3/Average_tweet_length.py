from __future__ import print_function
import sys
import json
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="avglength")
    tweets = sc.textFile(sys.argv[1], 40)
    lower = tweets.map(lambda word : word.lower())
    lower.cache()
    tweet_text = lower.map(lambda l :json.loads(l)["text"])
    tweet_length = tweet_text.map(lambda li :len(li))
    avg_tweet_length = tweet_length.sum()/float(tweet_length.count())
    prez = lower.filter(lambda tweet : '211178363' in tweet)
    prez_tweets= prez.map(lambda line : json.loads(line)["text"])
    prez_length = prez_tweets.map(lambda line : len(line))
    avg_prez_length = prez_length.sum()/float(prez_length.count())
    print("Average tweet length of PrezOno: %d" % avg_prez_length)
    other_tweets = lower.filter(lambda k :k!='211178363')
    other_tweets= prez.map(lambda li : json.loads(li)["text"])
    other_tweets_length = other_tweets.map(lambda le : len(le))
    avg_others_tweet_length = other_tweets_length.sum()/float(other_tweets_length.count())
    print("Average length of others: %d" % avg_others_tweet_length)
      

    sc.stop()