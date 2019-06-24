# Spark example to print the Centroid of Latitude and Longitude using Spark
# PGT April 2016   
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/

from __future__ import print_function
import sys, json, operator
from pyspark import SparkContext
from operator import add

  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="LatitudeandLongitude")
  
  tweets = sc.textFile(sys.argv[1],)
  
  geo = tweets.map(lambda line:json.loads(line)["geo"]) # Extracting all the geo details
  filtered = geo.filter(lambda keyvalue: keyvalue!=None) # Filtering out the None values
  Latitude=filtered.map(lambda key : key['coordinates'][0]) # Latitude
  Longitude=filtered.map(lambda key : key['coordinates'][1]) # Longitude
  
  count=float(Latitude.count()) # Tweets with location count
  b=filtered.count() # Tweets with location count
  c=geo.count()     # Total number of tweets count
  
  
  lats = Latitude.map(lambda s: ("Latitude",float(s/count))).reduceByKey(add) 
  longs =Longitude.map(lambda s: ("Longitude",float(s/count))).reduceByKey(add)
 
  
  # Prints the centroid of location 
  print ("Location Centroid", [lats.first()[1],longs.first()[1]])
  print ("The Proportion of tweets with location to those without %d : %d" % (b,c-b))
  # Print out the Proportion
 
  
  # Save to your localt HDFS folder
  lats.saveAsTextFile("Latitude")
  longs.saveAsTextFile("Longitude")
  
  
  sc.stop()
  
