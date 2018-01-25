# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 10:43:26 2017

@author: angli
"""

import pickle
import datetime
import urllib.parse
import json
import csv
import os
import pandas as pd
import numpy as np

# read the list of users
os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/muslimban")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/muslimban")

###aggregation
data = pd.read_csv("contri_muslin.csv", encoding="latin-1") #activity data from year 2017
data.columns.values

user_info = pd.read_csv("user_muslBan.csv")
user_info.columns.values

##merge user_info with contri data
data = pd.merge(data, user_info, how='left', on=['wpid', 'userid'])
newcomersdata = data.loc[data['newcomers'] == 1]
newcomersdata.to_csv("newcomers_contri.csv", index=False)


#formatting

data["timestamp"]=data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y %H:%M:%S'))
data['day'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %H:%M:%S').strftime('%m/%d/%Y'))
#data['DoW'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %H:%M:%S').strftime('%A'))
#change to timestamp for furture process
data['timestamp'] = pd.to_datetime(data['timestamp'])


#iterrows to fetch the sanbox
data['sandbox'] = 0
for index, row in data.iterrows():
    title = row["title"]
    ns = row["ns"]
    if ns == 2:
        title_lst = title.split("/")
        if "sandbox" in title_lst:
            data.set_value(index,"sandbox",1)

data.columns.values

daylist = len(set(list(data['day'])))

###group the data per day for contri only in 11 articles
article_list = ['Executive Order 13769', 
                "Executive Order 13780", 
                "Protests against Executive Order 13769", 
                "List of protests against Executive Order 13769", 
                "Reactions to Executive Order 13769",
                "Legal challenges to Executive Order 13769", 
                "Dismissal of Sally Yates", 
                "Washington v. Trump", 
                "Legal challenges to Executive Orders 13769 and 13780",
                "Immigration policy of Donald Trump", 
                "First 100 days of Donald Trump's presidency",
                "Day Without Immigrants 2017"]

data = pd.read_csv("contri_muslin.csv", encoding="latin-1") #activity data from year 2017
data.columns.values

"First 100 days of Donald Trump's presidency" in list(set(list(data['title'])))

data = data[data['title'].isin(article_list)]
user_info = pd.read_csv("user_muslBan.csv")
user_info.columns.values

##merge user_info with contri data
data = pd.merge(data, user_info, how='left', on=['wpid', 'userid'])

data["timestamp"]=data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y %H:%M:%S'))
data['day'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %H:%M:%S').strftime('%m/%d/%Y'))
#data['DoW'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %H:%M:%S').strftime('%A'))
#change to timestamp for furture process
data['timestamp'] = pd.to_datetime(data['timestamp'])

aggre_data={}
#group by day
Grouped = data.groupby(['day'])

n=0
for daygroup in Grouped:
    n+=1
    #if n==3: break
    day = daygroup[0]
    aggre_data[day]={}
    day_data = daygroup[1][['wpid','newcomers']]
    unique_wpids = day_data.drop_duplicates()
    numof_editors = len(list(unique_wpids['wpid']))
    numof_newcomers = unique_wpids['newcomers'].sum()
    aggre_data[day]['numof_editors'] = numof_editors
    aggre_data[day]['numof_newcomers'] = numof_newcomers
    #total edit count
    edit_count = len(list(daygroup[1]['wpid']))
    aggre_data[day]['edit_count'] = edit_count
    #ave size diff
    ave_sizediff = daygroup[1]['sizediff'].mean()
    aggre_data[day]['ave_sizediff'] = ave_sizediff
    #ave article size diff
    all_df = daygroup[1][['ns','sizediff']]
    article_df = all_df.loc[all_df['ns'] == 0]
    article_sizediff = article_df['sizediff'].mean()
    aggre_data[day]['article_sizediff'] = article_sizediff
    # article type count
    articleType_list = list(daygroup[1]['ns'])
    article_count = articleType_list.count(0) #edits in article
    article_index = [i for i in range(len(articleType_list)) if articleType_list[i]==0]
    talk_count = articleType_list.count(1)
    user_count = articleType_list.count(2)
    usertalk_count = articleType_list.count(3)
    aggre_data[day]['article_count'] = article_count
    aggre_data[day]['talk_count'] = talk_count
    aggre_data[day]['user_count'] = user_count
    aggre_data[day]['usertalk_count'] = usertalk_count
    #number of articles
    title_lst = list(daygroup[1]['title'])
    article_lst = list(set([title_lst[j] for j in article_index]))
    unique_article_numbers = len(article_lst)
    aggre_data[day]['unique_articles'] = unique_article_numbers
    aggre_data[day]['unique_articles_list'] = article_lst
    # sandbox edit
    aggre_data[day]['sandbox_count'] = daygroup[1]["sandbox"].sum()    
    #Minor edit
    minor_lst = list(daygroup[1]['minor'])
    minor_count = len(minor_lst)-minor_lst.count('not')
    aggre_data[day]['minor_count'] = minor_count
    #for newcomers
    try:
        newcomer_df = daygroup[1].loc[daygroup[1]['newcomers'] == 1.0]
        newcomer_edit_count = newcomer_df['newcomers'].sum()
        aggre_data[day]['newcomer_edit_count'] = newcomer_edit_count
        newcomer_ave_sizediff = newcomer_df['sizediff'].mean()
        aggre_data[day]['newcomer_ave_sizediff'] = newcomer_ave_sizediff
        newcomer_article_df = newcomer_df.loc[newcomer_df['ns'] == 0]
        newcomer_article_sizediff = newcomer_article_df['sizediff'].mean()
        aggre_data[day]['newcomer_article_sizediff'] = newcomer_article_sizediff
        newcomer_articleType_list = list(newcomer_df['ns'])
        newcomer_article_count = newcomer_articleType_list.count(0) #edits in article
        newcomer_article_index = [i for i in range(len(newcomer_articleType_list)) if newcomer_articleType_list[i]==0]
        newcomer_talk_count = newcomer_articleType_list.count(1)
        newcomer_user_count = newcomer_articleType_list.count(2)
        newcomer_usertalk_count = newcomer_articleType_list.count(3)
        aggre_data[day]['newcomer_article_count'] = newcomer_article_count
        aggre_data[day]['newcomer_talk_count'] = newcomer_talk_count
        aggre_data[day]['newcomer_user_count'] = newcomer_user_count
        aggre_data[day]['newcomer_usertalk_count'] = newcomer_usertalk_count
        newcomer_title_lst = list(newcomer_df['title'])
        newcomer_article_lst = list(set([newcomer_title_lst[j] for j in newcomer_article_index]))
        newcomer_unique_article_numbers = len(newcomer_article_lst)
        aggre_data[day]['newcomer_unique_articles'] = newcomer_unique_article_numbers
        aggre_data[day]['newcomer_unique_articles_list'] = newcomer_article_lst        
    except KeyError:
        aggre_data[day]['newcomer_edit_count'] = 0
        aggre_data[day]['newcomer_ave_sizediff'] = 0
        aggre_data[day]['newcomer_article_sizediff'] = 0
        aggre_data[day]['newcomer_article_count'] = 0
        aggre_data[day]['newcomer_talk_count'] = 0
        aggre_data[day]['newcomer_user_count'] = 0
        aggre_data[day]['newcomer_usertalk_count'] = 0
   
    if n%100==0: print (n)

day_aggre = pd.DataFrame.from_dict(aggre_data, orient='index')
day_aggre = day_aggre.fillna(value=0)
day_aggre.to_csv("editor_aggre_per_day_newcomers11pages.csv")
day_aggre.to_csv("editor_aggre_per_day_newcomers_all.csv")

pickle.dump( aggre_data, open( "day_aggre_all.p", "wb" ) )
day_aggre_all = pickle.load( open( "day_aggre_all.p", "rb" ) )

aggre_data['01/07/2017']['unique_articles_list']#'newcomer_unique_articles_list'
aggre_data['01/07/2017']['newcomer_unique_articles_list']#'newcomer_unique_articles_list'


aggre_data['04/27/2017']['newcomer_unique_articles']#'newcomer_unique_articles_list'

april_data = data.loc[data['day'] == '04/27/2017']


##########per day, per editor
###group the data per day
data = newcomersdata
data.columns.values

aggre_data={}
#group by day
Grouped = data.groupby(['day'])

n=0
for daygroup in Grouped:
    n+=1
    #if n==2: break
    day = daygroup[0]
    #day_data = daygroup[1][['wpid','newcomers']]
    #unique_wpids = day_data.drop_duplicates()
    #numof_editors = len(list(unique_wpids['wpid']))
    #numof_newcomers = unique_wpids['newcomers'].sum()
    aggre_data[day]={}
    
    #group on wpid
    wpidGrouped = daygroup[1].groupby(['wpid'])
    for wpidgroup in wpidGrouped:
        WPID = wpidgroup[0]
        aggre_data[day][WPID]={}
        #unique editors and newcomers for that date
        #aggre_data[day][WPID]['numof_editors'] = numof_editors
        #aggre_data[day][WPID]['numof_newcomers'] = numof_newcomers
        #studentID
        #studentID = list(wpidgroup[1]["wpid"])[0]
        #aggre_data[day][WPID]['wpid'] = studentID
        #userid
        userid = list(wpidgroup[1]["userid"])[0]
        aggre_data[day][WPID]['userid'] = userid
        #total edit count
        edit_count = len(list(wpidgroup[1]['wpid']))
        aggre_data[day][WPID]['edit_count'] = edit_count
        #ave size diff
        ave_sizediff = wpidgroup[1]['sizediff'].mean()
        aggre_data[day][WPID]['ave_sizediff'] = ave_sizediff
        #ave article size diff
        all_df = wpidgroup[1][['ns','sizediff']]
        article_df = all_df.loc[all_df['ns'] == 0]
        article_sizediff = article_df['sizediff'].mean()
        aggre_data[day][WPID]['article_sizediff'] = article_sizediff
        # article type count
        articleType_list = list(wpidgroup[1]['ns'])
        article_count = articleType_list.count(0) #edits in article
        article_index = [i for i in range(len(articleType_list)) if articleType_list[i]==0]
        talk_count = articleType_list.count(1)
        user_count = articleType_list.count(2)
        usertalk_count = articleType_list.count(3)
        aggre_data[day][WPID]['article_count'] = article_count
        aggre_data[day][WPID]['talk_count'] = talk_count
        aggre_data[day][WPID]['user_count'] = user_count
        aggre_data[day][WPID]['usertalk_count'] = usertalk_count
        #number of articles
        title_lst = list(wpidgroup[1]['title'])
        article_lst = list(set([title_lst[j] for j in article_index]))
        unique_article_numbers = len(article_lst)
        aggre_data[day][WPID]['unique_articles'] = unique_article_numbers
        # sandbox edit
        #aggre_data[day][WPID]['sandbox_count'] = wpidgroup[1]["sandbox"].sum()    
        # edit delta
        #time_data = pd.DataFrame(wpidgroup[1]['timestamp'])
        #time_data['timestamp_shift'] = time_data['timestamp'].shift(1)        
        #time_data["time_delta"] = time_data['timestamp'] - time_data['timestamp_shift']        
        #edit_delta = time_data['time_delta'].mean()
        #aggre_data[day][WPID]['edit_timedelta'] = edit_delta/np.timedelta64(1, 'm')
        # number of days first day till last day
        #timestamp_lst = list(coursegroup[1]['timestamp'])
        #number_days_firstlast = timestamp_lst[-1]-timestamp_lst[0]
        #aggre_data[wid][courseID]['num_days_last']=number_days_firstlast/np.timedelta64(1, 'D')#conversion to days
        #Minor edit
        #minor_lst = list(wpidgroup[1]['minor'])
        #minor_count = len(minor_lst)-minor_lst.count('not')
        #aggre_data[day][WPID]['minor_count'] = minor_count
        if n%100==0: print (n)
    
#convert aggre_data dictionary into dataframe
#convert ContactAve dictionary into dataframe        
names = []
frames = []
for name, d in aggre_data.items():    
    names.append(name)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))

#convert aggre_data dictionary into dataframe
df_Contact = pd.concat(frames, keys=names).sort_index()

#drop index into columns
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(level=0, inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'courseID'})
df_Contact = df_Contact.rename(columns={'index': 'wpid'})
df_Contact.columns.values

df_Contact.to_csv("newcomers_aggre_day.csv", index=False)


#change columns order
columns = ['wpid', 'courseID', 'userid', 
       'article_count', 'user_count', 'usertalk_count', 
       'minor_count', 'sandbox_count', 
       'edit_count', 'talk_count', 'unique_articles',
       'ave_sizediff', 'article_sizediff', 'edit_timedelta']

df_Contact = df_Contact[columns]

#write to csv
# df_PID.to_csv("all_aggre_data_9.20.csv", index=False)
df_Contact.to_csv("editor_aggre_per_person.csv", index=False)


###################

 
###group the data per person
aggre_data={}
#group by wpid
Grouped = data.groupby(['wpid'])

n=0
for pidgroup in Grouped:
    n+=1
    #if n==2: break
    wid = pidgroup[0]
    aggre_data[wid]={}
    
    #group on course
    courseGrouped = pidgroup[1].groupby(['userid'])
    for coursegroup in courseGrouped:
        courseID = coursegroup[0]
        aggre_data[wid][courseID]={}
        #studentID
        studentID = list(coursegroup[1]["wpid"])[0]
        aggre_data[wid][courseID]['wpid'] = studentID
        #course
        course = list(coursegroup[1]["userid"])[0]
        aggre_data[wid][courseID]['userid'] = course
        #total edit count
        edit_count = len(list(coursegroup[1]['wpid']))
        aggre_data[wid][courseID]['edit_count'] = edit_count
        #ave size diff
        ave_sizediff = coursegroup[1]['sizediff'].mean()
        aggre_data[wid][courseID]['ave_sizediff'] = ave_sizediff
        #ave article size diff
        all_df = coursegroup[1][['ns','sizediff']]
        article_df = all_df.loc[all_df['ns'] == 0]
        article_sizediff = article_df['sizediff'].mean()
        aggre_data[wid][courseID]['article_sizediff'] = article_sizediff
        # article type count
        articleType_list = list(coursegroup[1]['ns'])
        article_count = articleType_list.count(0) #edits in article
        article_index = [i for i in range(len(articleType_list)) if articleType_list[i]==0]
        talk_count = articleType_list.count(1)
        user_count = articleType_list.count(2)
        usertalk_count = articleType_list.count(3)
        aggre_data[wid][courseID]['article_count'] = article_count
        aggre_data[wid][courseID]['talk_count'] = talk_count
        aggre_data[wid][courseID]['user_count'] = user_count
        aggre_data[wid][courseID]['usertalk_count'] = usertalk_count
        #number of articles
        title_lst = list(coursegroup[1]['title'])
        article_lst = list(set([title_lst[j] for j in article_index]))
        unique_article_numbers = len(article_lst)
        aggre_data[wid][courseID]['unique_articles'] = unique_article_numbers
        # sandbox edit
        aggre_data[wid][courseID]['sandbox_count'] = coursegroup[1]["sandbox"].sum()    
        # edit delta
        time_data = pd.DataFrame(coursegroup[1]['timestamp'])
        time_data['timestamp_shift'] = time_data['timestamp'].shift(1)        
        time_data["time_delta"] = time_data['timestamp'] - time_data['timestamp_shift']        
        edit_delta = time_data['time_delta'].mean()
        aggre_data[wid][courseID]['edit_timedelta'] = edit_delta/np.timedelta64(1, 'm')
        # number of days first day till last day
        #timestamp_lst = list(coursegroup[1]['timestamp'])
        #number_days_firstlast = timestamp_lst[-1]-timestamp_lst[0]
        #aggre_data[wid][courseID]['num_days_last']=number_days_firstlast/np.timedelta64(1, 'D')#conversion to days
        #Minor edit
        minor_lst = list(coursegroup[1]['minor'])
        minor_count = len(minor_lst)-minor_lst.count('not')
        aggre_data[wid][courseID]['minor_count'] = minor_count
        if n%100==0: print (n)
    
#convert aggre_data dictionary into dataframe
#convert ContactAve dictionary into dataframe        
names = []
frames = []
for name, d in aggre_data.items():    
    names.append(name)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))

#convert aggre_data dictionary into dataframe
df_Contact = pd.concat(frames, keys=names).sort_index()

#drop index into columns
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(level=0, inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'courseID'})
df_Contact = df_Contact.rename(columns={'index': 'wpid'})
df_Contact.columns.values

#df_Contact.to_csv("survival_test_0225.csv", index=False)


#change columns order
columns = ['wpid', 'courseID', 'userid', 
       'article_count', 'user_count', 'usertalk_count', 
       'minor_count', 'sandbox_count', 
       'edit_count', 'talk_count', 'unique_articles',
       'ave_sizediff', 'article_sizediff', 'edit_timedelta']

df_Contact = df_Contact[columns]

#write to csv
# df_PID.to_csv("all_aggre_data_9.20.csv", index=False)
df_Contact.to_csv("editor_aggre_per_person.csv", index=False)


######
###group the data per title
aggre_data={}
#group by wpid
Grouped = data.groupby(['wpid'])

n=0
for pidgroup in Grouped:
    n+=1
    #if n==2: break
    wid = pidgroup[0]
    aggre_data[wid]={}
    
    #group on title
    courseGrouped = pidgroup[1].groupby(['title'])
    for coursegroup in courseGrouped:
        #title is courseID
        courseID = coursegroup[0]
        aggre_data[wid][courseID]={}
        #studentID
        studentID = list(coursegroup[1]["wpid"])[0]
        aggre_data[wid][courseID]['wpid'] = studentID
        #userid
        userid = list(coursegroup[1]["userid"])[0]
        aggre_data[wid][courseID]['userid'] = userid
        #total edit count
        edit_count = len(list(coursegroup[1]['wpid']))
        aggre_data[wid][courseID]['edit_count'] = edit_count
        #ave size diff
        ave_sizediff = coursegroup[1]['sizediff'].mean()
        aggre_data[wid][courseID]['ave_sizediff'] = ave_sizediff
        #ave article size diff
        all_df = coursegroup[1][['ns','sizediff']]
        article_df = all_df.loc[all_df['ns'] == 0]
        article_sizediff = article_df['sizediff'].mean()
        aggre_data[wid][courseID]['article_sizediff'] = article_sizediff
        # article type count
        articleType_list = list(coursegroup[1]['ns'])
        ns_type = articleType_list[0]
        article_count = articleType_list.count(0) #edits in article
        article_index = [i for i in range(len(articleType_list)) if articleType_list[i]==0]
        talk_count = articleType_list.count(1)
        user_count = articleType_list.count(2)
        usertalk_count = articleType_list.count(3)
        aggre_data[wid][courseID]['ns_type'] = ns_type
        aggre_data[wid][courseID]['article_count'] = article_count
        aggre_data[wid][courseID]['talk_count'] = talk_count
        aggre_data[wid][courseID]['user_count'] = user_count
        aggre_data[wid][courseID]['usertalk_count'] = usertalk_count
        #number of articles
        title_lst = list(coursegroup[1]['title'])
        article_lst = list(set([title_lst[j] for j in article_index]))
        unique_article_numbers = len(article_lst)
        aggre_data[wid][courseID]['unique_articles'] = unique_article_numbers
        # sandbox edit
        aggre_data[wid][courseID]['sandbox_count'] = coursegroup[1]["sandbox"].sum()    
        # edit delta
        time_data = pd.DataFrame(coursegroup[1]['timestamp'])
        time_data['timestamp_shift'] = time_data['timestamp'].shift(1)        
        time_data["time_delta"] = time_data['timestamp'] - time_data['timestamp_shift']        
        edit_delta = time_data['time_delta'].mean()
        aggre_data[wid][courseID]['edit_timedelta'] = edit_delta/np.timedelta64(1, 'm')
        # number of days first day till last day
        #timestamp_lst = list(coursegroup[1]['timestamp'])
        #number_days_firstlast = timestamp_lst[-1]-timestamp_lst[0]
        #aggre_data[wid][courseID]['num_days_last']=number_days_firstlast/np.timedelta64(1, 'D')#conversion to days
        #Minor edit
        minor_lst = list(coursegroup[1]['minor'])
        minor_count = len(minor_lst)-minor_lst.count('not')
        aggre_data[wid][courseID]['minor_count'] = minor_count
        if n%100==0: print (n)
    
#convert aggre_data dictionary into dataframe
#convert ContactAve dictionary into dataframe        
names = []
frames = []
for name, d in aggre_data.items():    
    names.append(name)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))

#convert aggre_data dictionary into dataframe
df_Contact = pd.concat(frames, keys=names).sort_index()

#drop index into columns
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(level=0, inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'courseID'})
df_Contact = df_Contact.rename(columns={'index': 'wpid'})
df_Contact.columns.values

#df_Contact.to_csv("survival_test_0225.csv", index=False)


#change columns order
columns = ['wpid',  'userid','courseID', 'ns_type',
       'article_count', 'user_count', 'usertalk_count', 
       'minor_count', 'sandbox_count', 
       'edit_count', 'talk_count', 'unique_articles',
       'ave_sizediff', 'article_sizediff', 'edit_timedelta']

df_Contact = df_Contact[columns]

#write to csv
# df_PID.to_csv("all_aggre_data_9.20.csv", index=False)
df_Contact.to_csv("editor_aggre_per_article.csv", index=False)
































