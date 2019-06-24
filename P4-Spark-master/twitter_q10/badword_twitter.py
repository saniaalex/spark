#spark-submit --master yarn-client bad.py hdfs://hadoop2-0-0/data/twitter
# sudo yum install python-matplotlib
from pyspark import SparkContext
import sys, math,json
from operator import add
import re
bad_list=[]
wordcount_list=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
total_list=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
bad_ratio=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
def f(x):  #function for calculating number of bad words and total number of words in each tweet
  count=0  
  length=0
  for i in x:
    length=length+1
    if i.lower() in bad_list:
      count=count+1
  return [(count,length)]
 
def g(t):  #function for extracting hour of tweet from key created_at 
  m=t[3]
  strhr=m[0]+m[1]
  num_hr=int(strhr)
  return [num_hr]
  

  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="bad_words")
  tweets = sc.textFile(sys.argv[1],)
#  tweets = tweets1.flatMap(getText)
  bad_words=sc.textFile("hdfs://hadoop2-0-0/user/alexsa/bad_wordlist")  #importing bad word list
  bad_wordlist=bad_words.map(lambda line:re.sub('[\W_]', '',line).split('\t'))
  bc=bad_wordlist.count() #finding length of the list of bad words
  full=bad_wordlist.take(bc)
  for i in range(0,bc):
    each_badword=full[i]
    str1 = re.sub('\W_', '',''.join(each_badword))  
    bad_list.append(str1)
    
  text1 = tweets.map(lambda line:json.loads(line)["text"].split()) # Extracting all the tweet text
  hour= tweets.map(lambda line:json.loads(line)["created_at"].split()) #Extracting the day/date/time 
  
  
  bad_count = text1.flatMap(f) #obtaining [number of bad words,total words] in each tweet
  hour_count=hour.flatMap(g) #obtaining hour from the tweet
  
  badfinal=bad_count.collect() #storing all [number of bad words,total words] into a list
  hourfinal=hour_count.collect() #storing hours of every tweet in a list
  count=len(hourfinal)
  for i in range(0,count):  #calculating bad-words proportion in every hour
    x=hourfinal[i]
    total_list[x]=total_list[x]+badfinal[i][1]
    wordcount_list[x]=wordcount_list[x]+badfinal[i][0]
    bad_ratio[x]=float(float(wordcount_list[x])/float(total_list[x]))
    
  for i in range(0,24): #printing proportion of bad words by every hour
    print bad_ratio[i]
  
    
  sc.stop() 
  
  