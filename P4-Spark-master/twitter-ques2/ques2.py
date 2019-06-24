from __future__ import print_function
import sys, json, operator
from pyspark import SparkContext
from operator import add
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="ques2")
  twitter_data = sc.textFile(sys.argv[1],)
  day_and_screenname = twitter_data.map(lambda line :  (json.loads(line)['created_at'].split()[0],json.loads(line)['user']['screen_name']))
  filtered=day_and_screenname.filter(lambda key : (key[1])=="PrezOno")
  final=filtered.map(lambda key : (key[0],float(1/52.0))).reduceByKey(add)
  final_out = final.collect()
  final.saveAsTextFile("ques2_twitter")
  print(final_out)
  sc.stop()
