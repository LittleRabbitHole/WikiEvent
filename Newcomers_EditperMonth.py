#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 14:48:39 2018

this is to prepare the newcomers monthly edits
for the first rq: activity timeline

@author: Ang
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
import itertools

os.chdir("/Users/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-BLM")
os.chdir("/Users/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-FIFA")
os.chdir("/Users/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-ebola")

data = pd.read_csv("event_newcomer_contri.csv")
data = data.dropna(subset=['title'])
data = data.reset_index(drop=True)
data['current_date'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
data['month'] = data['current_date'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y').strftime('%m/%Y'))
data["event_edit"] = data.apply(EventEdit, axis=1)
sum(data["event_edit"] )

#get the aggre data from contri data
aggre_month = Survival_aggre(data)

#
names = []
frames = []
for name, d in aggre_month.items():    
    names.append(name)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))

#pd.DataFrame.from_dict(aggre_data["1980na"], orient='index')
            
df_Contact = pd.concat(frames, keys=names).sort_index()
df_Contact.columns.values

#drop index into columns
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'month'})
df_Contact = df_Contact.rename(columns={'index': 'user'})
df_Contact.columns.values

df_Contact.to_csv("newcomer_monthly_edit.csv", index = False)

##this is to prepare the survival 
def Survival_aggre(data):  
	#input as contribution data
	#ouput as dict: wid:{time: summary}  
    aggre_data={}
    #group by wpid
    Grouped = data.groupby(['wpid'])
    
    n=0
    for pidgroup in Grouped:
        n+=1
        if n%10 == 0: print(n)
        #if n==2: break
        wid = pidgroup[0]
        sid = list(pidgroup[1]['userid'])[0]
        aggre_data[wid]={}
        # number of days first day till last day
        
        #group on survival time unit (day)
        time_grouped = pidgroup[1].groupby(['month'])
        for timegroup in time_grouped:
            time = timegroup[0]
            #time_name = "day"+str(time)
            aggre_data[wid][time]={}
            #looking at the accumilated articles
            #add sid
            aggre_data[wid][time]["userid"] = sid
            #add time index
            #add user global var

            #total edit count
            day_data = timegroup[1]
            edit_count = len(list(day_data['wpid']))
            aggre_data[wid][time]['edit_count'] = edit_count
            #ave article size diff
            # article type count
            day_data_articleType_list = list(day_data['ns'])
            day_data_article_count = day_data_articleType_list.count(0) #edits in article
            #day_data_article_index = [i for i in range(len(day_data_articleType_list)) if day_data_articleType_list[i]==0]
            day_data_talk_count = day_data_articleType_list.count(1)
            day_data_user_count = day_data_articleType_list.count(2)
            day_data_usertalk_count = day_data_articleType_list.count(3)
            aggre_data[wid][time]['article_count'] = day_data_article_count
            aggre_data[wid][time]['talk_count'] = day_data_talk_count
            aggre_data[wid][time]['user_count'] = day_data_user_count
            aggre_data[wid][time]['usertalk_count'] = day_data_usertalk_count
            #number of articles
            day_data_article_lst = list(day_data['title'].loc[day_data['ns']==0])
            day_data_unique_article_numbers = len(set(day_data_article_lst))
            aggre_data[wid][time]['unique_articles'] = day_data_unique_article_numbers
            # sandbox edit
            #aggre_data[wid][courseID]['sandbox_count'] = coursegroup[1]["sandbox"].sum()
            #event edits
            event_data = day_data.loc[day_data['event_edit']==1]
            event_edit = len(list(event_data['wpid']))
            event_data_articleType_list = list(event_data['ns'])
            event_edit_article_count = event_data_articleType_list.count(0)
            aggre_data[wid][time]['event_edit'] = event_edit
            aggre_data[wid][time]['event_article_count'] = event_edit_article_count
         
    return (aggre_data)