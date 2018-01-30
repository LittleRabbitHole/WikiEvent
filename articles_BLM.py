#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 13:26:56 2017
This is the data collection till survival prep for BLM
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
import Wiki_datacollections_all

#read list of articles
f = open("articles_full.csv")
articles = f.readlines()
f.close()

#collect article revisions
f = open("BLM_article_revisions.csv", "w", encoding="UTF-8")
f = open("BLM_article_revisions_ids.csv", "w", encoding="UTF-8")

csv_f = csv.writer(f)
#write first row
csv_f.writerow(['article_revision_timeindex','wpid','userid','article' ,'timestamp', 'revid', 'parentid'])

n=0
for article in articles:
    n+=1
    print (n, article)
    article = article.strip().replace(" ","_")
    revisions = GetRevisions(article)
    m=0
    for item in revisions:
        m+=1
        csv_f.writerow([m, item.get('user',''), item.get('userid',''), article, item.get('timestamp',''), item.get("revid",""), item.get("parentid","")])
    
f.close() 

##########################################
###get all users from article revisions#####################
##########################################

alluser_list = GetAritlceEdiors(articles)
#len(alluser_list["Shooting_of_Michael_Brown"])

##unique user list
names = []
for article, users in alluser_list.items(): 
    user_list = list(list(users for users,_ in itertools.groupby(users)))
    names += [user[0] for user in user_list]
    #frames.append(pd.DataFrame(d))

len(set(names))#9442

editors = list(set(names))

pickle.dump( editors, open( "user_list_blm_ferg.p", "wb" ) )


####collecting editor info##################
### user info
f = open("user_info_blm_event.csv", "w", encoding="UTF-8")
csv_f = csv.writer(f)
#write error
f_error=open("user_info_errors.csv", "w", encoding="UTF-8")
csv_error = csv.writer(f_error)

##collection editor info
#write first row
csv_f.writerow(['wpid','userid','editcount','gender', 'registration'])

editors = pickle.load( open( "user_list_blm_ferg.p", "rb" ) )

user_list = [x for x in editors if x != ""]
len(user_list)

# collecting user info: gender/registration date, etc
# read username catch the Wiki data
n=0
for name in user_list:
    userinfodata = GetUserInfo(name)#list
    try:
        csv_f.writerow([userinfodata['name'], userinfodata.get('userid'), userinfodata.get('editcount'), userinfodata.get('gender'), userinfodata['registration']])
    except KeyError:
        csv_error.writerow([name])
    n+=1
    if n%100==0:
        print (n) 

f.close()
f_error.close()


##########################################
###get all users- article matches#####################
##########################################
article_revisions = pd.read_csv("BLM_article_revisions.csv", encoding = "ISO-8859-1")
article_revisions['time'] = article_revisions['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
article_revisions['time'] = pd.to_datetime(article_revisions['time'])
article_revisions.columns.values
article_revisions = article_revisions[['wpid', 'userid', 'article','time']]

article_revisions.to_csv("article_revisions_window.csv", index=False)

article_editor_revisions = article_revisions[['wpid', 'userid', 'article']]

df = article_editor_revisions.drop_duplicates()
df = df.loc[df['userid'] != 0]
df.to_csv("eventcoop_BLM_editors.csv", index=False)


#####second filtering for newcomers who join within 2 weeks after event timeline
f = open("Timeline2.csv")
time_intervals = f.readlines()
f.close()

user_info = pd.read_csv("user_info_blm_event.csv", encoding = "ISO-8859-1")
user_info = user_info.dropna()
user_info["registration_date"]=user_info['registration'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
user_info['registration_date'] = pd.to_datetime(user_info['registration_date'])

n=0
newcomers_mark=[]
for index, row in user_info.iterrows():
    n+=1
    if n%100==0: print(n)
    newcomer = 0
    for period in time_intervals:
        period = period.strip()
        time_lst = period.split(",")
        start_time = pd.to_datetime(time_lst[1])
        end_time = pd.to_datetime(time_lst[2])
        
        if row["registration_date"] <= end_time and row["registration_date"] >= start_time:
            newcomer+=1
    if newcomer>=1: 
        check = 1
    else:
        check = 0
    newcomers_mark.append(check)

user_info['newcomer_mark'] = newcomers_mark
user_info.to_csv("blm_user_info_newcomers.csv", index=False)


##################
##################
#new column based on register-event(article first edit) time different
user_info = pd.read_csv("blm_user_info_newcomers.csv", encoding = "ISO-8859-1")
user_info = user_info.dropna()
user_info["registration_date"]=user_info['registration'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
user_info['registration_date'] = pd.to_datetime(user_info['registration_date'])
user_info.columns.values
user_info = user_info[["wpid","userid","registration_date",'newcomer_mark']]

article_revisions = pd.read_csv("BLM_article_revisions.csv", encoding = "ISO-8859-1")
article_revisions.columns.values
article_info = article_revisions[['article','timestamp']].loc[article_revisions['article_revision_timeindex'] == 1]
article_info["firstedit_date"]=article_info['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
article_info.to_csv("article_info_blm_event.csv", index=False)


article_user_match = pd.read_csv("eventcoop_BLM_editors.csv", encoding = "ISO-8859-1")
user_data = pd.merge(article_user_match, user_info, how='left', on=['wpid', 'userid'])
userarticle_data = pd.merge(user_data, article_info, how='left', on=['article'])
userarticle_data.columns.values

userarticle_data['registration_date'] = pd.to_datetime(userarticle_data['registration_date'])
userarticle_data['firstedit_date'] = pd.to_datetime(userarticle_data['firstedit_date'])
#userarticle_data["register-event"] = (userarticle_data['registration_date'] - userarticle_data['firstedit_date'])/np.timedelta64(1, 'D')
#userarticle_data["newcomers-time"] =  userarticle_data.apply(timeNewcomers, axis=1)#1=within 1 month; 2 after
userarticle_data['registration_year'] = userarticle_data['registration_date'].dt.year
userarticle_data.columns.values
userarticle_data = userarticle_data.dropna()

userarticle_data.to_csv("userarticle_blm_newcomers.csv", index=False)

###newcomers contribution collections
#####list of registered newers
userarticle_data = pd.read_csv("userarticle_blm_newcomers.csv", encoding = "ISO-8859-1", dtype=object)
newcomer_list = userarticle_data[["wpid","userid"]].loc[userarticle_data['newcomer_mark'] == "1"]
newcomer_list = newcomer_list.drop_duplicates()

##################
###lsit of time based newcomers
newusers = list(newcomer_list["userid"])

###newcomers contribution collections
f = open("event_newcomer_contri_blm.csv", "w", encoding="UTF-8")
csv_f = csv.writer(f)
#write first row
csv_f.writerow(['userid','userid', 'wpid', 'timestamp', 'ns',  'title', 'sizediff', 'minor'])

#write error
f_error=open("newcomer_info_errors.csv", "w", encoding="UTF-8")

csv_error = csv.writer(f_error)


n=0
for user in newusers:
    n+=1
    print (n, user)
    usercontri = GetUserContri(user)
    if usercontri == "erroruser":
        csv_error.writerow([user])
    else:
        usercontribsdata = usercontri
        for feature in usercontribsdata:
            csv_f.writerow([user, feature['userid'], feature['user'], feature['timestamp'],feature['ns'],feature['title'],
                            feature.get('sizediff'), feature.get('minor', 'not')])    
    #if n%10==0:

f.close()

####user contribution########
####set of newcomers-event who come from first event articles

user_contri = pd.read_csv("event_newcomer_contri_blm.csv", encoding = "ISO-8859-1")
user_contri['index'] = user_contri.index
user_contri.columns.values

#find out event-newcomer: who edit direct the event articles
user_contri_articles = user_contri.loc[user_contri['ns'] == 0]
titles = list(set(list(user_contri_articles["title"])))

user_contri_articles["shift"] = user_contri_articles['wpid'].shift(1)
user_contri_articles["firstarticle_mark"] = np.where(user_contri_articles['wpid'] !=  user_contri_articles['shift'],1,0)
user_contri_articles["event_newcomers"] = user_contri_articles.apply(Event, axis=1)

event_newcomers_df = user_contri_articles[["wpid","userid","event_newcomers"]].loc[user_contri_articles['event_newcomers'] == 1]

user_info = pd.read_csv("userarticle_blm_newcomers.csv", encoding = "ISO-8859-1")
newcomer_info = user_info.loc[user_info['newcomer_mark'] != 0]
full_newcomers_type = pd.merge(newcomer_info, event_newcomers_df, how='left', on=['wpid', 'userid'])

#check duplicates in different article
full_newcomers_type = full_newcomers_type[["wpid","userid","registration_date",'newcomer_mark','registration_year','event_newcomers']].drop_duplicates()
full_newcomers_type = full_newcomers_type.sort_values(["wpid","userid",'newcomer_mark'], ascending=[True, True, True])
full_newcomers_type["shift"] = full_newcomers_type['userid'].shift(1)
full_newcomers_type["duplicate_mark"] = np.where(full_newcomers_type['userid'] ==  full_newcomers_type['shift'],0,1)
full_newcomers_type = full_newcomers_type.loc[full_newcomers_type['duplicate_mark'] == 1]
full_newcomers_type['event_newcomers'].fillna(0, inplace=True)
#newcomers-time: newcomer based on time window--1:1month;2larger; event_newcomers:based on first edit
full_newcomers_type[["wpid","userid","registration_date",'newcomer_mark','registration_year','event_newcomers']].to_csv("newcomer_usertype.csv", index=False)

#contri_user type
full_newcomers_type = pd.read_csv("newcomer_usertype.csv", encoding = "ISO-8859-1")
full_newcomers_type.columns.values
full_newcomers_type['registration_date'] = pd.to_datetime(full_newcomers_type['registration_date'])
user_info = full_newcomers_type[["wpid","userid","registration_date",'newcomer_mark','registration_year','event_newcomers']]
user_info = user_info.drop_duplicates()
user_contri_usertype = pd.merge(user_contri, user_info, how='inner', on=['wpid', 'userid'])

user_contri_usertype.columns.values

user_contri_usertype.to_csv("event_newcomer_contri_blm_usertype.csv", index=False)

###for survival
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

df_Contact.to_csv("blm_survival_prep.csv", index=False)

survival = df_Contact.loc[df_Contact['last_edit_mark'] == 1]
survival.to_csv("blm_survival_final.csv", index=False)

#####second filtering for newcomers who join within 2 weeks after event timeline
f = open("Timeline.csv")
time_intervals = f.readlines()
f.close()

user_info = pd.read_csv("user_info_blm_event.csv", encoding = "ISO-8859-1")
user_info = user_info.dropna()
user_info["registration_date"]=user_info['registration'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
user_info['registration_date'] = pd.to_datetime(user_info['registration_date'])

n=0
newcomers_mark=[]
for index, row in user_info.iterrows():
    n+=1
    if n%100==0: print(n)
    newcomer = 0
    for period in time_intervals:
        period = period.strip()
        time_lst = period.split(",")
        start_time = pd.to_datetime(time_lst[1])
        end_time = pd.to_datetime(time_lst[2])
        
        if row["registration_date"] <= end_time and row["registration_date"] >= start_time:
            newcomer+=1
    if newcomer>=1: 
        check = 1
    else:
        check = 0
    newcomers_mark.append(check)

user_info['newcomer_mark'] = newcomers_mark
user_info.to_csv("blm_user_info_newcomers.csv", index=False)
