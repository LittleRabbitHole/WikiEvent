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
import argparse
import logging

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger('get_pageview')


if __name__ == "__main__":

    # Global settings
    # parser
    parser = argparse.ArgumentParser(description='Input Year')
    parser.add_argument('-y', '--year', help='input year to locate into correct file location')
    args = parser.parse_args()
    year = args.year
    
    #logger
    logger = logging.getLogger('get_pageview')
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    #read articles
    f = open("/home/angatpitt/wikievent/all133articles.txt")
    articles = f.readlines()
    f.close()
    
    article_list = [article.strip().split("\t")[0] for article in articles]
    event_list = [article.strip().split("\t")[1] for article in articles]
    
    #process the file
    file_loc = "/home/angatpitt/wikidumpdata/pageview_{}".format(str(year))
    input_LOCATION_All = "{}/pagecounts-*".format(file_loc)
    #each around 130,000,000 entries
    
    for file in glob.glob(input_LOCATION_All):
        current = os.path.join(input_LOCATION_All, file)
        _log.info("Reading From {}...".format(current))
        
        #current = "/Users/angli/Desktop/pagecounts-2014-04-views-ge-5-totals"
        filename = os.path.basename(current)
        month = filename[-25:-18]
        #if os.path.isfile(current):
        
        outString = ""
        with open(current) as infile: #errors='ignore'
            n=0
            for line in infile:
                n+=1
                if n%10000000==0: print(n)
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

        #creating output file
        _log.info("Generating output file {}...".format(filename))
             
        result_path = '/home/angatpitt/wikidumpdata/results'
        if not os.path.exists(result_path):
            os.makedirs(result_path)
    
        with open('{}/pageview_{}.txt'.format(result_path, filename[-25:-18]), 'w') as f:
            f.write(outString)
            f.close()
    
        _log.info("Output file created for {}...".format(filename))
    