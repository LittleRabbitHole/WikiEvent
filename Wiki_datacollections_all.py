# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 10:01:00 2017
This is for all the re0usable functions
for import and use for other code
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

#this is to retrive all revisions for one article
def GetRevisions(pageTitle):
    #pageTitle = 'Executive_Order_13769'
    #https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=Executive_Order_13769&rvprop=timestamp|user|comment&generator=revisions
    url = "https://en.wikipedia.org/w/api.php?action=query&format=json&rvdir=newer&rvlimit=500&prop=revisions&rvprop=ids|timestamp|user|userid|comment&titles=" + pageTitle
    revisions = []                                        #list of all accumulated revisions
    next = ''                                             #information for the next request
    while True:
        #response = urllib2.urlopen(url + next).read()     #web request
        response=urllib.request.urlopen(url + next)
        str_response=response.read().decode('utf-8')
        responsedata = json.loads(str_response)
        page_id = list(responsedata["query"]["pages"].keys())[0]
        revision_data_lst=responsedata['query']['pages'][page_id]['revisions']
        revisions += revision_data_lst  #adds all revisions from the current request to the list

        try:        
            cont = responsedata['continue']['rvcontinue']
        except KeyError:                                      #break the loop if 'continue' element missing
            break

        next = "&rvcontinue=" + cont             #gets the revision Id from which to start the next request

    return revisions

#this is from article get the revision
#from revision, store in dictionary as:
# ... article1: [user, userid], ...
#eliminated bots
def GetAritlceEdiors(article_list):
    alluser_list = {}
    for article in article_list:
        article = article.strip().replace(" ","_")
        revisions = GetRevisions(article)
        users = []
        #m=0
        for item in revisions:
            username = item.get('user','')
            if ('bot' not in username) and ('Bot' not in username) and ('BOT' not in username):
                users.append([item.get('user',''),item.get('userid','')])
    
        user_list = list(list(users for users,_ in itertools.groupby(users)))
        print (article, len(user_list))
        alluser_list[article] = user_list
    return alluser_list


#this is to get each user's information
def GetUserInfo(name):
    wpid =  name
    wpid = wpid.strip()
    #print (rawID)
    wpid_nospace = wpid.replace(" ","_")
    #decode student's name into ascii
    decode_wpid = urllib.parse.quote(wpid_nospace)
    #api
    #api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=users&ususers={}&usprop=editcount|blockinfo|registration|rights|groups|gender&format=json").format(decode_wpid)#Kingsleyta
    api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=users&ususers={}&usprop=editcount|registration|gender&format=json").format(decode_wpid)#Kingsleyta
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    #try:
    userinfodata=responsedata["query"]["users"][0]#list
    return userinfodata
    
#within certain time window
def isInWindow(c):
    if c['registration_date'] >= pd.to_datetime("2014-06-01") and c['registration_date'] <= pd.to_datetime("2015-06-01"): 
        return (1)
    else:
        return (0)

def timeNewcomers(c):
    if c['register-event'] <= 30 and c['register-event'] >= -11: 
    #if c['register-event'] >= -11: 
        return (1)
    elif c['register-event'] > 30:
        return (2)
    else:
        return (0)

def isEarlyExperience(c):
    if c['time_index'] > 0: 
        return (1)
    else:
        return (0)

def isOneMonth(c):
    if c['time_index'] <= 15: 
        return (1)
    else:
        return (0)


def RevisionWindow(c):
    if c['time'] >= pd.to_datetime("2014-01-01") and c['time'] <= pd.to_datetime("2016-01-01"): 
        return (1)
    else:
        return (0)
        

#including event words
def Event(c):
    #eventword_lst = ['ferguson','shoot','death','black lives','shooting', 'murder','protest','rally','unrest', 'hands up','movement','protests','riots','riot','killing','byp100','african americans']
    eventword_lst = ['ebola','operation united assistance','body team 12','ouse to ouse tock', 'vaccine', 'VP40', 'womey massacre']
    #eventword_lst = ['fifa','world cup','caxirola','we are one','belo horizonte overpass collapse','dar um jeito', 'nippon (song)','brazil','controversies']
    if c["firstarticle_mark"] == 1 and any(word in c['title'].lower() for word in eventword_lst):
        return (1)
    else:
        return (0)


def EventEdit(c):
    #eventword_lst = ['ferguson','shoot','death','black lives','shooting', 'murder','protest','rally','unrest', 'hands up','movement','protests','riots','riot','killing']
    #eventword_lst = ['ebola','operation united assistance','body team 12','ouse to ouse tock', 'vaccine', 'VP40', 'womey massacre']
    eventword_lst = ['fifa','world cup','caxirola','we are one','belo horizonte overpass collapse','dar um jeito','nippon (song)','brazil','controversies']
    if any(word in c['title'].lower() for word in eventword_lst):
        return (1)
    else:
        return (0)


def FirstEdit(c):
    #eventword_lst = ['ferguson','shoot','death','black lives','shooting', 'murder','protest','rally','unrest', 'hands up','movement','protests','riots','riot','killing']
    #eventword_lst = ['ebola','operation united assistance','body team 12','ouse to ouse tock', 'vaccine', 'VP40', 'womey massacre']
    eventword_lst = ['fifa','world cup','caxirola','we are one','belo horizonte overpass collapse','dar um jeito','nippon (song)','brazil','controversies']
    if c['ns'] == 0: 
        if any(word in c['title'].lower() for word in eventword_lst):
            return (1) #event article
        else:
            return (2)#articles
    elif c['ns'] == 1 or c['ns'] == 3:
        return (3) #talk
    elif c['ns'] == 2:
        return (4) #user page
    else:
        return (5)


#this is to get each editor's contribution over history
def GetUserContri(name,starttime,endtime):
    wpid =  name
    wpid = wpid.strip()
    #print (rawID)
    #wpid_nospace = wpid.replace(" ","_")
    #decode student's name into ascii
    #decode_wpid = urllib.parse.quote(wpid_nospace)
    decode_wpid = urllib.parse.quote(wpid)
    #api
    #api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucstart=2014-01-01T00:00:00Z&ucuser={}&ucdir=newer&ucprop=title|timestamp|size|sizediff|flags&uclimit=500&format=json").format(decode_wpid)#Kingsleyta  
    #use username
    #api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuser={}&ucdir=newer&ucprop=title|timestamp|size|sizediff|flags&uclimit=500&format=json").format(decode_wpid)#Kingsleyta  
    #use userid
    #api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuserids={}&ucdir=newer&ucprop=title|timestamp|ids|size|sizediff|flags&uclimit=500&format=json").format(decode_wpid)#Kingsleyta  
    #use userid, collect only articles,return with revid IDs
    api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuserids={}&ucdir=newer&ucstart={}&ucend={}&ucnamespace=0&ucprop=title|timestamp|ids|size|sizediff|flags&uclimit=500&format=json").format(decode_wpid,starttime,endtime)#Kingsleyta  

    user_contribs = []  
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    if 'error' in list(responsedata.keys()):
        return "erroruser"
    else:
        next = ''                                           #information for the next request
        m=0
        while True:
            m+=1
            print (m)
            response=urllib.request.urlopen(api_call + next)
            str_response=response.read().decode('utf-8')
            responsedata = json.loads(str_response)
            usercontribsdata=responsedata["query"]["usercontribs"]#list
            user_contribs += usercontribsdata
            try:        
                cont = responsedata['continue']['uccontinue']
            except KeyError:                                      #break the loop if 'continue' element missing
                break
        
            next = "&uccontinue=" + cont             #gets the revision Id from which to start the next request
    return (user_contribs)


##this is to prepare the survival 
def Survival_aggre(data):    
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
        event_newcomer = pidgroup[1]['event_newcomers'].mean()
        time_newcomer = pidgroup[1]['newcomers-time'].mean()
        newcomer_registyear = list(pidgroup[1]['registration_year'])[0]
        newcomer_regist = list(pidgroup[1]['registration_date'])[0]
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
            #looking at the accumilated articles
            acc_article_df = pidgroup[1].loc[pidgroup[1]['ns']==0]
            to_today = acc_article_df.loc[acc_article_df['day_index']<= time]
            acc_articles = len(set(list(to_today['title'])))
            aggre_data[wid][time]['totoday_articles']=acc_articles
            #newcomer type
            aggre_data[wid][time]['event_newcomers'] = event_newcomer
            aggre_data[wid][time]['time_newcomer'] = time_newcomer
            aggre_data[wid][time]['newcomer_registyear'] = newcomer_registyear
            aggre_data[wid][time]['newcomer_regist'] = newcomer_regist
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

            #total edit count
            day_data = timegroup[1]
            edit_count = len(list(day_data['wpid']))
            aggre_data[wid][time]['edit_count'] = edit_count
            #ave size diff
            ave_sizediff = sum(list(day_data['sizediff']))/(len(list(day_data['sizediff'])) + 0.0001)
            #print (day_data_ave_sizediff)
            aggre_data[wid][time]['ave_sizediff'] = ave_sizediff
            #ave article size diff
            day_data_df = day_data[['ns','sizediff']]
            day_data_article_df = day_data_df.loc[day_data_df['ns'] == 0]
            day_data_article_sizediff = sum(list(day_data_article_df['sizediff']))/(len(list(day_data_article_df['sizediff'])) + 0.0001) #day_data_article_df['sizediff'].mean()
            aggre_data[wid][time]['article_sizediff'] = day_data_article_sizediff
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

         
    return (aggre_data)
