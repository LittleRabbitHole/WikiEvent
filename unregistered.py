# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 13:16:18 2017

@author: angli
"""

import pandas as pd
import os
import datetime
import numpy as np
import urllib.parse
import json
import csv
import os
import urllib
import pickle
# read the list of users
users = pd.read_csv("charlots_allusertype.csv", encoding = "ISO-8859-1")
users.columns.values

unregistered_users = users.loc[users['newcomer'] == -1]

users = list(unregistered_users["wpid"])


f = open("contri_unregistered.csv", "w", encoding="UTF-8")
csv_f = csv.writer(f)
#write first row
csv_f.writerow(['wpid','userid', 'user', 'timestamp', 'ns',  'title', 'size', 'sizediff', 'comment', 'minor'])


#user = users[1]

# read username catch the Wiki data
n=0
for user in users:
    wpid =  user
    wpid = wpid.strip()
    #print (rawID)
    wpid_nospace = wpid.replace(" ","_")
    #decode student's name into ascii
    decode_wpid = urllib.parse.quote(wpid_nospace)
    #api
    api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuser={}&ucdir=newer&ucprop=title|timestamp|comment|size|sizediff|flags&uclimit=500&format=json").format(decode_wpid)#Kingsleyta
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    usercontribsdata=responsedata["query"]["usercontribs"]#list
    #write into csv
    #csv_f.writerow(['userid', 'user', 'timestamp', 'ns',  'title', 'size', 'sizediff', 'comment', 'minor'])
    for feature in usercontribsdata:
        csv_f.writerow([wpid, feature['userid'], feature['user'], feature['timestamp'],feature['ns'],feature['title'],
                            feature.get('size'),feature.get('sizediff'),feature.get('comment'), feature.get('minor', 'not')])
        
    n+=1
    if n%10==0:
        print (n) #2416

f.close()
