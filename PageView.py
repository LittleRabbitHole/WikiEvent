#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 12:32:16 2018
@author: angli

traverse all the pagecount wiki dump and find the articles that listed in the "articles133.txt"
"""

from __future__ import division
import glob
import os


f = open("all133articles.txt")
articles = f.readlines()
f.close()

article_list = [article.strip().split("\t")[0] for article in articles]
event_list = [article.strip().split("\t")[1] for article in articles]

file_loc = "/home/angatpitt/wikidump"
input_LOCATION_All = "{}/pagecounts-*".format(file_loc)

for file in glob.glob(input_LOCATION_All):
    current = os.path.join(input_LOCATION_All, file)
    #current = "/Users/angli/Desktop/pagecounts-2014-04-views-ge-5-totals"
    filename = os.path.basename(current)
    month = filename[-25:-18]
    #if os.path.isfile(current):
    outString = ""
    with open(current, errors='ignore') as infile:
        n=0
        for line in infile:
            n+=1
            if n%100000==0: print(n)
            line_lst = line.strip().split(" ")
            project = line_lst[0].lower()
            title = line_lst[1]
            count = line_lst[2]
            if (project == "en.z") and (title in article_list): #
                ind = article_list.index(title)
                event = event_list[ind]
                print (line)
                outString += '\t'.join([month, event, project, title, count])
                outString += '\n'
                #csv_f = csv.writer(f)
                #write first row
                #csv_f.writerow([ title,count, filename])
                #break


with open('/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/test.txt', 'w') as f:
    f.write(outString)
    f.close()
    