#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 12:32:16 2018
@author: angli

traverse all the pagecount wiki dump and find the articles that listed in the "articles133.txt"
"""

from __future__ import division
import glob
import urllib
import pickle
import os

file_loc = "/home/angatpitt/wikidump"
input_LOCATION_All = "{}/pagecounts-*".format(file_loc)

for file in glob.glob(input_LOCATION_All):
    current = os.path.join(input_LOCATION_All, file)
    filename = os.path.basename(current)
    #if os.path.isfile(current):
    f = open(current) 
    tweettext = f.read()
    f.close()
   
f = open("BLM_article_pageview.csv", "w", encoding="UTF-8")

current = "/Users/angli/Desktop/pagecounts-2014-04-views-ge-5-totals"
with open(current, errors='ignore') as infile:
    n=0
    for line in infile:
        n+=1
        if n%100000==0: print(n)
        line_lst = line.split(" ")
        project = line_lst[0].lower()
        title = line_lst[1]
        count = line_lst[2]
        if project == "en.z" and title == "Shooting_of_Oscar_Grant": #
            print (line)
            #csv_f = csv.writer(f)
            #write first row
            #csv_f.writerow([ title,count, filename])
            break
    