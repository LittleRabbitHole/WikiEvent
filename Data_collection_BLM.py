# -*- coding: utf-8 -*-
"""
Created on Sat Jul 22 14:31:17 2017

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

import Wiki_datacollections_all

# read the list of users
os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/blm")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/blm")

#BLM article list

article_list = [ 'Shooting_of_Oscar_Grant'] #01/01/2009

article_list =  ['Shooting_of_Trayvon_Martin', 'George_Zimmerman'] /#02/26/2012

article_list_ferg = [
                'Shooting_of_Michael_Brown', 
                'Ferguson_unrest',
                'Black_Lives_Matter'] #08/08/2014

article_list = ['Death_of_Eric_Garner','Black_Lives_Matter']#07/17/2014

                 
               'Death_of_Freddie_Gray',
                '2015_Baltimore_protests',
                
                'Death_of_Sandra_Bland',
                'Black_Lives_Matter'
###############################################################
##write all historical page revisions#####################

#f = open("ferg_article_revisions.csv", "w", encoding="UTF-8")
#f = open("eric_article_revisions.csv", "w", encoding="UTF-8")
#f = open("Oscar_article_revisions.csv", "w", encoding="UTF-8")

f = open("Oscar_article_revisions.csv", "w", encoding="UTF-8")

csv_f = csv.writer(f)
#write first row
csv_f.writerow(['wpid','userid','article' ,'timestamp', 'size','comment'])

for article in article_list:
    print (article)
    revisions = GetRevisions(article)
    #m=0
    for item in revisions:
        #m+=1
        csv_f.writerow([item.get('user',''), item.get('userid',''), article, item.get('timestamp',''), item.get('size',''),item.get('comment','')])
    
f.close()    

##########################################
###get all users from article revisions#####################
##########################################

alluser_list = GetAritlceEdiors(article_list )
#len(alluser_list["Shooting_of_Michael_Brown"])

##unique user list
names = []
for article, users in alluser_list.items(): 
    user_list = list(list(users for users,_ in itertools.groupby(users)))
    names += [user[0] for user in user_list]
    #frames.append(pd.DataFrame(d))

len(set(names))#2013

editors = list(set(names))

pickle.dump( editors, open( "user_list_blm_ferg.p", "wb" ) )
pickle.dump( editors, open( "user_list_blm_eric.p", "wb" ) )
pickle.dump( editors, open( "user_list_blm_oscar.p", "wb" ) )


#convert ContactAve dictionary into dataframe        
names = []
frames = []
for name, d in alluser_list.items(): 
    print (name)
    #print (d)
    names.append(name)
    frames.append(pd.DataFrame(d))

#convert aggre_data dictionary into dataframe
df_Contact = pd.concat(frames, keys=names).sort_index()

#drop index into columns
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(level=0, inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'id'})
df_Contact = df_Contact.rename(columns={'index': 'article'})
df_Contact.columns.values

#df_Contact.to_csv("ferg_BLM_editors.csv", index=False)
#df_Contact.to_csv("eric_BLM_editors.csv", index=False)
df_Contact.to_csv("oscar_BLM_editors.csv", index=False)



####blm#######
####collecting editor info##################
### user info
#f = open("user_info_blm_ferg.csv", "w", encoding="UTF-8")
#f = open("user_info_blm_eric.csv", "w", encoding="UTF-8")

f = open("user_info_blm_oscar.csv", "w", encoding="UTF-8")

csv_f = csv.writer(f)
#write error
f_error=open("user_info_errors.csv", "w", encoding="UTF-8")
csv_error = csv.writer(f_error)

##collection editor info
#write first row
csv_f.writerow(['wpid','userid','editcount','gender', 'registration'])

#editors = pickle.load( open( "user_list_blm_ferg.p", "rb" ) )

#editors = pickle.load( open( "user_list_blm_eric.p", "rb" ) )

editors = pickle.load( open( "user_list_blm_oscar.p", "rb" ) )

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

##################
##################
#newcomer section

#user_info = pd.read_csv("user_info_blm_ferg.csv", encoding = "ISO-8859-1")
#user_info = pd.read_csv("user_info_blm_eric.csv", encoding = "ISO-8859-1")

user_info = pd.read_csv("user_info_blm_oscar.csv", encoding = "ISO-8859-1")

user_info["registration_date"]=user_info['registration'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))

user_info['registration_date'] = pd.to_datetime(user_info['registration'])

user_info["within4month"] = user_info.apply(isInWindow, axis=1)
user_info.columns.values

#user_info.to_csv("user_info_blm_ferg.csv", index=False)
#user_info.to_csv("user_info_blm_eric.csv", index=False)
user_info.to_csv("user_info_blm_oscar.csv", index=False)

#####list of registered newers
newcomer_ferg_list = user_info[["wpid","userid"]].loc[user_info['within4month'] == 1]

##################
####################################
####################################
####collection newcomers contribution########
#user_info = pd.read_csv("user_info_blm_ferg.csv", encoding = "ISO-8859-1")
#user_info = pd.read_csv("user_info_blm_eric.csv", encoding = "ISO-8859-1")

user_info = pd.read_csv("user_info_blm_oscar.csv", encoding = "ISO-8859-1")

user_info.columns.values

#newcomers list
newcomer_data = user_info.loc[user_info['within4month'] == 1]
newusers = list(newcomer_data["wpid"])


#f = open("ferg_newcomer_contri_blm.csv", "w", encoding="UTF-8")
#f = open("eric_newcomer_contri_blm.csv", "w", encoding="UTF-8")

f = open("oscar_newcomer_contri_blm.csv", "w", encoding="UTF-8")

csv_f = csv.writer(f)
#write first row
csv_f.writerow(['wpid','userid', 'user', 'timestamp', 'ns',  'title', 'sizediff', 'minor'])

#write error
#f_error=open("ferg_newcomer_info_errors.csv", "w", encoding="UTF-8")
#f_error=open("eric_newcomer_info_errors.csv", "w", encoding="UTF-8")
f_error=open("newcomer_info_errors.csv", "w", encoding="UTF-8")

csv_error = csv.writer(f_error)


n=0
for user in newusers:
    n+=1
    usercontri = GetUserContri(user)
    if usercontri == "erroruser":
        csv_error.writerow([user])
    else:
        usercontribsdata = usercontri
        for feature in usercontribsdata:
            csv_f.writerow([user, feature['userid'], feature['user'], feature['timestamp'],feature['ns'],feature['title'],
                            feature.get('sizediff'), feature.get('minor', 'not')])    
    #if n%10==0:
    print (n)

f.close()

####user contribution########
####set of newcomers who come from first event articles

#user_contri = pd.read_csv("ferg_newcomer_contri_blm.csv", encoding = "ISO-8859-1")
#user_contri = pd.read_csv("eric_newcomer_contri_blm.csv", encoding = "ISO-8859-1")

user_contri = pd.read_csv("oscar_newcomer_contri_blm.csv", encoding = "ISO-8859-1")

user_contri.columns.values

user_contri_articles = user_contri.loc[user_contri['ns'] == 0]
user_contri_articles["shift"] = user_contri_articles['wpid'].shift(1)
user_contri_articles["firstarticle_mark"] = np.where(user_contri_articles['wpid'] !=  user_contri_articles['shift'],1,0)
user_contri_articles["event_newcomers"] = user_contri_articles.apply(Event, axis=1)

event_newcomers_df = user_contri_articles[["wpid","userid","event_newcomers"]].loc[user_contri_articles['event_newcomers'] == 1]

user_contri_usertype =  pd.merge(user_contri, event_newcomers_df, how='left', on=['wpid', 'userid'])
user_contri_usertype['event_newcomers'].fillna(0, inplace=True)

#user_contri_usertype.to_csv("eric_newcomer_contri_blm_usertype.csv", index=False)
#user_contri_usertype.to_csv("ferg_newcomer_contri_blm_usertype.csv", index=False)

user_contri_usertype.to_csv("oscar_newcomer_contri_blm_usertype.csv", index=False)


###############for surv data##################
#################
##merge user_info with contri data
user_info = pd.read_csv("user_info_blm_all.csv", encoding = "ISO-8859-1")
user_info.columns.values
user_info = user_info[['wpid', 'registration_date', 'event_newcomers']]

##get only the newcomers data
newcomersdata = pd.read_csv("newcomer_contri_blm.csv", encoding = "ISO-8859-1")
newcomersdata.columns.values

data = pd.merge(newcomersdata, user_info, how='left', on=['wpid'])
data.columns.values

##get only the newcomers data
newcomersdata = data.loc[data['event_newcomers'] == 1]
list(newcomersdata)

##newcomer data aggregation on dayindex
data = newcomersdata
data["timestamp"]=data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y %H:%M:%S'))
data['day'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %H:%M:%S').strftime('%m/%d/%Y'))
#data['DoW'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %H:%M:%S').strftime('%A'))
#change to timestamp for furture process
data['day'] = pd.to_datetime(data['day'])
data['registration_date'] = pd.to_datetime(data['registration_date'])
data["day_index"] = (data['day'] - data['registration_date'])/np.timedelta64(1, 'D')

#################
########for survival#########
#################
##read
#data = pd.read_csv("newcomers_editing_forsurv.csv", encoding="latin-1") #activity data from year 2017
data.columns.values

##prepare for survival
#change to timestamp for furture process
data['last_day_censored'] = "2017-09-14"

data["last_day_censored"] = pd.to_datetime(data["last_day_censored"])
data["day_index"] = data["time_index"]
data.columns.values

#data["wpid"] = data["TH"]
#data["SID"] = data["userid"]


data.columns.values    

aggre_data={}
#group by wpid
Grouped = data.groupby(['wpid'])

n=0
for pidgroup in Grouped:
    n+=1
    #if n==2: break
    wid = pidgroup[0]
    sid = list(pidgroup[1]['userid'])[0]
    newcomer_type = pidgroup[1]['event_newcomers'].mean()
    aggre_data[wid]={}
    # number of days first day till last day
    day_lst = list(pidgroup[1]['day'])
    first_survivalday = day_lst[0]
    last_survivalday = day_lst[-1]
    
    #group on survival time unit (day)
    time_grouped = pidgroup[1].groupby(['day_index'])
    for timegroup in time_grouped:
        time = timegroup[0]
        #time_name = "day"+str(time)
        aggre_data[wid][time]={}
        #newcomer type
        aggre_data[wid][time]['event_newcomers'] = newcomer_type
        #add sid
        aggre_data[wid][time]["userid"] = sid
        #add time index
        #aggre_data[wid][time]["time_index"] = time
        #add user global var
        last_edit = last_survivalday
        aggre_data[wid][time]["last_edit"] = last_edit # user last edit
        last_censored = list(timegroup[1]['last_day_censored'])[0]
        aggre_data[wid][time]["last_censored"] = last_censored
        #add current date
        today_date = list(timegroup[1]['day'])[0]
        aggre_data[wid][time]["today"] = today_date
        edit_times = len(timegroup[1])
        aggre_data[wid][time]["edit"] = edit_times
        #last edit till end of censored
        last_edit_tillend = (last_censored - last_edit)/np.timedelta64(1, 'D')
        aggre_data[wid][time]["lastedit_tillend"] = last_edit_tillend
        #mark the last edit
        if today_date == last_survivalday: 
            aggre_data[wid][time]["last_edit_mark"]=1
        else:
            aggre_data[wid][time]["last_edit_mark"]=0
        if today_date == last_survivalday and last_edit_tillend>30:
            aggre_data[wid][time]["death"]=1
        else:
            aggre_data[wid][time]["death"]=0
 
#convert aggre_data dictionary into dataframe
#convert ContactAve dictionary into dataframe        
names = []
frames = []
for name, d in aggre_data.items():    
    names.append(name)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))

#pd.DataFrame.from_dict(aggre_data["1980na"], orient='index')
            
df_Contact = pd.concat(frames, keys=names).sort_index()

#drop index into columns
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(level=0, inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'time'})
df_Contact = df_Contact.rename(columns={'index': 'user'})
df_Contact.columns.values


df_Contact["today"] = pd.to_datetime(df_Contact["today"])
df_Contact["last_censored"] = pd.to_datetime(df_Contact["last_censored"])
df_Contact["last_edit"] = pd.to_datetime(df_Contact["last_edit"])
df_Contact["last_edit_tillend"] = (df_Contact["last_censored"] - df_Contact["last_edit"])/np.timedelta64(1, 'D')#conversion to days
df_Contact["current_till_last_edit"] = (df_Contact["last_edit"] - df_Contact["today"])/np.timedelta64(1, 'D')#conversion to days

df_Contact.to_csv("blm_survival_prep.csv", index=False)

