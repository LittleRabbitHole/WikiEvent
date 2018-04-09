#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 13:47:29 2018
@author: angli

This is to prepare all the links that need to be download from Wikidump

"""

if __name__ == "__main__":

    outString = ''
    
    for y in range(2013, 2016): 
        year = '{num:04d}'.format(num=y)
        for m in range(1, 13):
            month = '{num:02d}'.format(num=m)                
            download_str = "https://dumps.wikimedia.org/other/pagecounts-ez/merged/pagecounts-{}-{}-views-ge-5-totals.bz2".format(year, month)
            outString += download_str
            outString += '\n'
    
    with open('/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/Wikidump_monthly.txt', 'w') as f:
        f.write(outString)
        f.close()
    
#wget -i Wikidump.txt -P /home/angatpitt/wikidumpdata
#https://dumps.wikimedia.org/other/pagecounts-ez/merged/pagecounts-2014-04-views-ge-5-totals.bz2