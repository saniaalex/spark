#!/usr/bin/env python

import sys
import string
import re
import matplotlib.pyplot as plt
day_hour=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
badcount_ratio=[]
syl_a=[]


for line in sys.stdin:
  badcount = line.strip().split('\t')
  badcount_ratio.append(badcount)
  
print len(badcount_ratio)
  
 
plt.plot(day_hour,badcount_ratio)
plt.xlabel('Hour of day')
plt.ylabel('Bad word proportion')
plt.title('Bad word proportion in tweets VS Hour of day')
plt.show()