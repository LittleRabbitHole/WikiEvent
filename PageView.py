#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 12:32:16 2018

@author: angli
"""

import pandas as pd
import os
import numpy as np
import urllib.parse
import json
import csv
import os
import urllib
import pickle
import itertools


f = open("BLM_article_pageview.csv", "w", encoding="UTF-8")

filename = "/Users/angli/Desktop/pagecounts-20140801-000000"
with open("/Users/angli/Desktop/pagecounts-20140801-000000", errors='ignore') as infile:
    n=0
    for line in infile:
        n+=1
        if n%10000==0: print(n)
        line_lst = line.split(" ")
        project = line_lst[0]
        title = line_lst[1]
        count = line_lst[2]
        if project == "en" and title == "Death_of_Eric_Garner":
            print (line)
            csv_f = csv.writer(f)
            #write first row
            csv_f.writerow([ title,count, filename])
            break
    