#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 12:00:32 2018

@author: angli
"""

from __future__ import division
import glob
import os

if __name__ == "__main__":

    #process the file
    file_loc = "/home/angatpitt/wikidumpdata/results"
    input_LOCATION_All = "{}/*.txt".format(file_loc)
    #each around 130,000,000 entries
    
    outString = ""
    for file in glob.glob(input_LOCATION_All):
        current = os.path.join(input_LOCATION_All, file)        
        #current = "/Users/angli/Desktop/pagecounts-2014-04-views-ge-5-totals"
        filename = os.path.basename(current)
        #if os.path.isfile(current):
        
        f = open(current) 
        monthly_pageview = f.read()
        f.close()
        
        outString += monthly_pageview
    
    with open('{}/pageview_2013-2015.txt'.format(file_loc), 'w') as f:
       f.write(outString)
       f.close()
