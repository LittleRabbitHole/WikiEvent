# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 14:15:43 2017

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
import itertools

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-BLM")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-BLM")

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-FIFA")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-FIFA")

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-ebola")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-ebola")

###############################################################
###############################################################
#mark the first edit as event edit/article edit/userpage/talkpage
data = pd.read_csv("event_newcomer_contri_blm_usertype.csv", encoding = "ISO-8859-1")
data = pd.read_csv("event_newcomer_contri_usertype.csv", encoding = "ISO-8859-1")
data = data.dropna()
len(set(list(data['userid'])))

data.columns.values
data["last_userid"] = data['userid'].shift(1)
fist_edit_data = data.loc[data["last_userid"] != data['userid']]

fist_edit_data["first_edit_type"] = fist_edit_data.apply(FirstEdit, axis=1)
fist_edit_data.columns.values
fist_edit_data.to_csv("newcomer_firstedit.csv", index=True)

###############################################################
#####seperate the first event article editing####
#mark the first event
data = pd.read_csv("event_newcomer_contri_blm_usertype.csv", encoding = "ISO-8859-1")
data = pd.read_csv("event_newcomer_contri_usertype.csv", encoding = "ISO-8859-1")
data = data.dropna()

data.columns.values
data["event_edit"] = data.apply(EventEdit, axis=1)

data['current_date'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
data['current_date'] = pd.to_datetime(data['current_date'])
data['registration_date'] = pd.to_datetime(data['registration_date'])
data['time_index'] = (data['current_date'] - data['registration_date'])/np.timedelta64(1, 'D')
data["last_wpid"] = data['userid'].shift(1)

#mark before and after event edit
after_mark=[]
marker =0
for index, row in data.iterrows():
    if row["last_wpid"] == row['userid']:
        if marker == 1:
            after_mark.append(1)
        elif marker == 0:
            if row['event_edit'] == 0:
                after_mark.append(0)
            elif row['event_edit'] == 1:
                after_mark.append(1)
                marker = 1
    else:
        marker = 0
        if row['event_edit'] == 0:
            after_mark.append(0)
        elif row['event_edit'] == 1:
            after_mark.append(1)
            marker = 1

data["after_first_event_edit"] = after_mark
data["compare"] = data['after_first_event_edit'].shift(1)


userids = data[['userid','wpid']].drop_duplicates()
userids.to_csv("newcomer_userids.csv", index=True)

#mark the first event
first_event_edit = data.loc[(data["compare"] != data["after_first_event_edit"]) | (data["last_wpid"] != data["userid"])]
len(first_event_edit['userid'].drop_duplicates())
first_event_edit.to_csv("newcomer_first_event_edit.csv", index=True)

###############################################################
###############################################################
###to filtering out late adopter > 30 days ###to filtering out late adopter > 30 days
#select the final newcomers
first_event_edit = pd.read_csv("newcomer_first_event_edit.csv")
final_newcomers = first_event_edit.loc[(first_event_edit['event_edit'] == 1) & (first_event_edit['time_index'] <= 120)]
final_newcomers.to_csv("newcomers_eventmark.csv", index=False)
#=IF(ISNA(VLOOKUP(B:B,[newcomers_eventmark.csv]newcomers_eventmark!$B:$B,1,0)),0,1)
#data.to_csv("newcomer_contri_blm_aftermark.csv", index=False)


########for per day per user data aggregation###################
########for per day per user data aggregation###################
########for per day per user data aggregation###################
data = pd.read_csv("event_newcomer_contri_blm_usertype.csv", encoding = "ISO-8859-1")
data['newcomers-time'] = data['newcomer_mark']
data.columns.values

data["timestamp"]=data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y %H:%M:%S'))
data['day'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %H:%M:%S').strftime('%m/%d/%Y'))

#change to timestamp for furture process
data['day'] = pd.to_datetime(data['day'])
data['registration_date'] = pd.to_datetime(data['registration_date'])
data["day_index"] = (data['day'] - data['registration_date'])/np.timedelta64(1, 'D')
sum(n < 0 for n in list(data["day_index"]))
data = data.loc[data['day_index'] >= 0]

data['last_day_censored'] = "2017-10-19"
data["last_day_censored"] = pd.to_datetime(data["last_day_censored"])

data.columns.values

#using the surcical function
aggre_surv = Survival_aggre(data)

#convert aggre_data dictionary into dataframe
#convert ContactAve dictionary into dataframe        
names = []
frames = []
for name, d in aggre_surv.items():    
    names.append(name)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))

#pd.DataFrame.from_dict(aggre_data["1980na"], orient='index')
            
df_Contact = pd.concat(frames, keys=names).sort_index()
df_Contact.columns.values

#drop index into columns
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'time'})
df_Contact = df_Contact.rename(columns={'index': 'user'})
df_Contact.columns.values

columns = ['user', 'userid', 'last_edit_mark',
           'time', 'today', 'last_edit', 'death', 'newcomer_registyear', 
           'time_newcomer','event_newcomers','last_censored',
           'ave_sizediff', 
           'edit', 'edit_count',
           'user_count', 
           'totoday_articles','unique_articles',
           'article_sizediff',
           'article_count',  
           'usertalk_count',
           'talk_count', 
           'newcomer_regist', 
           'lastedit_tillend']

df_Contact = df_Contact[columns]



df_Contact.to_csv("blm_daily_activity.csv", index=False)



###############################################################
###############################################################
###############################################################

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-BLM")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-BLM")

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-FIFA")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-FIFA")

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-ebola")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-ebola")

###############################################################
##aggregation based on certain time point
data = pd.read_csv("event_newcomer_contri_blm_usertype.csv", encoding = "ISO-8859-1")

data = pd.read_csv("event_newcomer_contri_usertype.csv", encoding = "ISO-8859-1")

data.columns.values
data['current_date'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
data['current_date'] = pd.to_datetime(data['current_date'])
data['registration_date'] = pd.to_datetime(data['registration_date'])
data['time_index'] = (data['current_date'] - data['registration_date'])/np.timedelta64(1, 'D')
#set the time separation of event article editing
#data["Early_experience"] = data.apply(isEarlyExperience, axis=1)

#set the time separation of 1 month
data["Early_experience"] = data.apply(isOneMonth, axis=1)

########################################################################
###aggregation of before and after
#data = pd.read_csv("newcomer_contri_blm_aftermark.csv", encoding = "ISO-8859-1")

data.columns.values
data[0:10]

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
    
    #select on before and after
    piddata = pidgroup[1]#.groupby(['after_first_event_edit'])    
    studentID = list(piddata["wpid"])[0]
    aggre_data[wid]['WPID'] = studentID    
    #usertype
    user_type = list(piddata['event_newcomers'])[0]
    aggre_data[wid]['user_type'] = user_type        
    #register date
    register = list(piddata['registration_date'])[0]
    aggre_data[wid]['register'] = register       
    #userid
    userid = list(piddata["userid"])[0]
    aggre_data[wid]['userid'] = userid
    
    #before data
    #before = piddata.loc[piddata['after_first_event_edit'] == 0]
    before = piddata.loc[piddata['Early_experience'] == 1]

    #total edit count
    before_edit_count = len(list(before['wpid']))
    aggre_data[wid]['before_edit_count'] = before_edit_count
    #ave size diff
    before_ave_sizediff = sum(list(before['sizediff']))/(len(list(before['sizediff'])) + 0.0001)
    #print (before_ave_sizediff)
    aggre_data[wid]['before_ave_sizediff'] = before_ave_sizediff
    #ave article size diff
    before_df = before[['ns','sizediff']]
    before_article_df = before_df.loc[before_df['ns'] == 0]
    before_article_sizediff = sum(list(before_article_df['sizediff']))/(len(list(before_article_df['sizediff'])) + 0.0001) #before_article_df['sizediff'].mean()
    aggre_data[wid]['before_article_sizediff'] = before_article_sizediff
    # article type count
    before_articleType_list = list(before['ns'])
    before_article_count = before_articleType_list.count(0) #edits in article
    before_article_index = [i for i in range(len(before_articleType_list)) if before_articleType_list[i]==0]
    before_talk_count = before_articleType_list.count(1)
    before_user_count = before_articleType_list.count(2)
    before_usertalk_count = before_articleType_list.count(3)
    aggre_data[wid]['before_article_count'] = before_article_count
    aggre_data[wid]['before_talk_count'] = before_talk_count
    aggre_data[wid]['before_user_count'] = before_user_count
    aggre_data[wid]['before_usertalk_count'] = before_usertalk_count
    #number of articles
    before_article_lst = list(before['title'].loc[before['ns']==0])
    before_unique_article_numbers = len(set(before_article_lst))
    aggre_data[wid]['before_unique_articles'] = before_unique_article_numbers
    # sandbox edit
    #aggre_data[wid][courseID]['sandbox_count'] = coursegroup[1]["sandbox"].sum()    
    # edit delta
    before_time_index = list(before['time_index'])
    before_time_span = max(before_time_index, default=0) - min(before_time_index, default=0)
    aggre_data[wid]['before_time_span'] = before_time_span
    
    #after data
    #after = piddata.loc[piddata['after_first_event_edit'] == 1]
    after = piddata.loc[piddata['Early_experience'] == 0]

    #total edit count
    after_edit_count = len(list(after['wpid']))
    aggre_data[wid]['after_edit_count'] = after_edit_count
    #ave size diff
    after_ave_sizediff = sum(list(after['sizediff']))/(len(list(after['sizediff'])) + 0.0001)
    aggre_data[wid]['after_ave_sizediff'] = after_ave_sizediff
    #ave article size diff
    after_df = after[['ns','sizediff']]
    after_article_df = after_df.loc[after_df['ns'] == 0]
    after_article_sizediff = sum(list(after_article_df['sizediff']))/(len(list(after_article_df['sizediff'])) + 0.0001)
    aggre_data[wid]['after_article_sizediff'] = after_article_sizediff
    # article type count
    after_articleType_list = list(after['ns'])
    after_article_count = after_articleType_list.count(0) #edits in article
    after_article_index = [i for i in range(len(after_articleType_list)) if after_articleType_list[i]==0]
    after_talk_count = after_articleType_list.count(1)
    after_user_count = after_articleType_list.count(2)
    after_usertalk_count = after_articleType_list.count(3)
    aggre_data[wid]['after_article_count'] = after_article_count
    aggre_data[wid]['after_talk_count'] = after_talk_count
    aggre_data[wid]['after_user_count'] = after_user_count
    aggre_data[wid]['after_usertalk_count'] = after_usertalk_count
    #number of articles
    after_article_lst = list(after['title'].loc[after['ns']==0])
    after_unique_article_numbers = len(set(after_article_lst))
    aggre_data[wid]['after_unique_articles'] = after_unique_article_numbers
    # sandbox edit
    #aggre_data[wid][courseID]['sandbox_count'] = coursegroup[1]["sandbox"].sum()    
    # edit delta
    after_time_index = list(after['time_index'])
    after_time_span = max(after_time_index, default=0) - min(after_time_index, default=0)
    aggre_data[wid]['after_time_span'] = after_time_span


    if n%100==0: print (n)

bf_data = pd.DataFrame.from_dict(aggre_data, orient='index')
#change columns order

columns = ["WPID", 'userid','user_type','register',
'before_time_span',
'before_ave_sizediff',
'before_article_sizediff',
'before_unique_articles',
'before_article_count',
'before_edit_count',
'before_talk_count',
'before_user_count',
'before_usertalk_count',
'after_time_span',
'after_ave_sizediff',
'after_article_sizediff',
'after_unique_articles',
'after_article_count',
'after_edit_count',
'after_talk_count',
'after_user_count',
'after_usertalk_count']

bf_data = bf_data[columns]

#bf_data.to_csv("editor_aggre_firstday.csv", index=False)
bf_data.to_csv("editor_aggre_15day.csv", index=False)
#bf_data.to_csv("editor_aggre_30day.csv", index=False)

bf_data = bf_data.rename(columns={'WPID': 'wpid'})
bf_data.to_csv("editor_aggre_1month.csv", index=False)
bf_data.columns.values
###
#surv_data = pd.read_csv("blm_survival_final_unblock.csv")   
#surv_data = pd.read_csv("survival_final_FIFA_unblock.csv")   
surv_data = pd.read_csv("survival_final_ebola_unblock.csv")   
surv_data.columns.values
surv_data = surv_data[['user', 'userid', 'is_in_event', 'time', 'death','event_newcomers']].loc[surv_data['is_in_event'] == 1]
surv_data = surv_data.rename(columns={'user': 'wpid'})

final_data = pd.merge(surv_data, bf_data, how='left', on=['wpid', 'userid'])
final_data.to_csv("first_30days_factors.csv", index=False)
###
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
    



########################################################################
##aggregation of the article contri by day
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/2014-ebola")

data = pd.read_csv("article_revisions_usertype.csv", encoding = "ISO-8859-1") #all user edits in 10 articles
data.columns.values

data['date'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))

##############################
aggre_data={}
#group by day
Grouped = data.groupby(['date'])
n=0
for daygroup in Grouped:
    n+=1
    #if n==3: break
    day = daygroup[0]
    aggre_data[day]={}
    day_data = daygroup[1]
    unique_wpids = day_data.drop_duplicates()
    numof_editors = len(list(unique_wpids['wpid']))
    aggre_data[day]['numof_editors'] = numof_editors
    #for newcomers
    try:
        newcomer_df = daygroup[1].loc[daygroup[1]['newcomer'] == 1]
        numof_newcomer = len(set(list(newcomer_df['wpid'])))
        aggre_data[day]['numof_newcomer'] = numof_newcomer        
        newcomer_edit_count = newcomer_df['newcomer'].count()
        aggre_data[day]['newcomer_edit_count'] = newcomer_edit_count
    except KeyError:
        aggre_data[day]['newcomer_edit_count'] = 0
    #for unregistered
#    try:
#        unreg_df = daygroup[1].loc[daygroup[1]['newcomer'] == -1.0]
#        numof_unreg = len(set(list(unreg_df['wpid'])))
#        aggre_data[day]['numof_unreg'] = numof_unreg
#        unreg_edit_count = unreg_df['newcomer'].count()
#        aggre_data[day]['unreg_edit_count'] = unreg_edit_count
#    except KeyError:
#        aggre_data[day]['unreg_edit_count'] = 0
     #for old-timer
    try:
        oldtimer_df = daygroup[1].loc[daygroup[1]['newcomer'] == 0]
        numof_oldtimer = len(set(list(oldtimer_df['wpid'])))
        aggre_data[day]['numof_oldtimer'] = numof_oldtimer
        oldtimer_edit_count = oldtimer_df['newcomer'].count()
        aggre_data[day]['oldtimer_edit_count'] = oldtimer_edit_count
    except KeyError:
        aggre_data[day]['oldtimer_edit_count'] = 0
  
    if n%100==0: print (n)

day_aggre = pd.DataFrame.from_dict(aggre_data, orient='index')
day_aggre = day_aggre.fillna(value=0)
day_aggre.to_csv("revisions_aggre_per_day.csv")
