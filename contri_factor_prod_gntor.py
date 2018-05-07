#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 12:05:10 2018

This is the main process to generate the early-on experience factors 
and the subsequent contribution productions for each newcomer

@author: angli
"""

import os
import pandas as pd
import datetime
import numpy as np

if __name__ == "__main__": 
    #os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
    #os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")

    data_loc = "/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data"
    blm_event_loc = "2014-BLM"
    fifa_event_loc = "2014-FIFA"
    ebola_event_loc = "2014-ebola"


    users = pd.read_csv("Newcomers_groups.csv", encoding = "ISO-8859-1")

    blm_users = users[['wpid', 'userid', 'first_edit_type3']].loc[users['event'] == "BLM"]
    ebola_users = users[['wpid', 'userid', 'first_edit_type3']].loc[users['event'] == "Ebola"]
    fifa_users = users[['wpid', 'userid', 'first_edit_type3']].loc[users['event'] == "FIFA"]


    blm_contri = mergeUsers(data_loc, event_loc = blm_event_loc, usergroup = blm_users)
    fifa_contri = mergeUsers(data_loc, event_loc = fifa_event_loc, usergroup = fifa_users)
    ebola_contri = mergeUsers(data_loc, event_loc = ebola_event_loc, usergroup = ebola_users)

    #checkBalance(blm_contri)
    #checkBalance(fifa_contri)
    #checkBalance(ebola_contri)


    ###aggregation of before and after

    blm_factors = aggre_sep(blm_contri, event="blm")
    fifa_factors = aggre_sep(fifa_contri, event="fifa")
    ebola_factors = aggre_sep(ebola_contri, event="ebola")

    all_week_factors = pd.concat([blm_factors, fifa_factors, ebola_factors])

    all_week_factors.to_csv("firstweek_factors_outcomes.csv", index = False)


def isOneMonth(c):
    if c['time_index'] <= 7: 
        return (1)
    else:
        return (0)

##merge data with the users I need

def mergeUsers(data_loc, event_loc, usergroup):
    data = pd.read_csv("{}/{}/event_newcomer_contri_usertype.csv".format(data_loc, event_loc), encoding = "ISO-8859-1")
    data.columns.values
    data = pd.merge(data , usergroup, on=['wpid', 'userid'])
    
    data['current_date'] = data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
    data['current_date'] = pd.to_datetime(data['current_date'])
    data['registration_date'] = pd.to_datetime(data['registration_date'])
    data['time_index'] = (data['current_date'] - data['registration_date'])/np.timedelta64(1, 'D')
    #set the time separation of event article editing
    #data["Early_experience"] = data.apply(isEarlyExperience, axis=1)
    
    #set the time separation of 1 month
    data["Early_experience"] = data.apply(isOneMonth, axis=1)
    
    return (data)


def checkBalance(contri_data):
    print ("total: ", len(set(list(contri_data["userid"]))))
    print ("early: ", len(set(list(contri_data["userid"].loc[contri_data['Early_experience'] == 1]))))
    print ("after: ", len(set(list(contri_data["userid"].loc[contri_data['Early_experience'] == 0]))))



def aggre_sep(data, event):
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
        aggre_data[wid]['wpid'] = studentID    
        #usertype
        user_type = list(piddata['first_edit_type3'])[0]
        aggre_data[wid]['first_edit_type3'] = user_type        
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
        
        #ave article size diff
        before_df = before[['ns','sizediff']]
        before_article_df = before_df.loc[before_df['ns'] == 0]
        before_article_sizediff = sum(list(before_article_df['sizediff']))/(len(list(before_article_df['sizediff'])) + 0.0001) #before_article_df['sizediff'].mean()
        aggre_data[wid]['before_article_sizediff'] = before_article_sizediff
        # article type count
        before_articleType_list = list(before['ns'])
        before_article_count = before_articleType_list.count(0) #edits in article
        #before_article_index = [i for i in range(len(before_articleType_list)) if before_articleType_list[i]==0]
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
        
        #after data
        #after = piddata.loc[piddata['after_first_event_edit'] == 1]
        after = piddata.loc[piddata['Early_experience'] == 0]
    
        #total edit count
        after_edit_count = len(list(after['wpid']))
        aggre_data[wid]['after_edit_count'] = after_edit_count
        #ave article size diff
        after_df = after[['ns','sizediff']]
        after_article_df = after_df.loc[after_df['ns'] == 0]
        after_article_sizediff = sum(list(after_article_df['sizediff']))/(len(list(after_article_df['sizediff'])) + 0.0001)
        aggre_data[wid]['after_article_sizediff'] = after_article_sizediff
        # article type count
        after_articleType_list = list(after['ns'])
        after_article_count = after_articleType_list.count(0) #edits in article
        #after_article_index = [i for i in range(len(after_articleType_list)) if after_articleType_list[i]==0]
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
        
        if n%100==0: print (n)
    
    bf_data = pd.DataFrame.from_dict(aggre_data, orient='index')
    #change columns order
    bf_data["event"] = event
    columns = ["wpid", 'userid','event','first_edit_type3','register',
    'before_article_sizediff',
    'before_unique_articles',
    'before_article_count',
    'before_edit_count',
    'before_talk_count',
    'before_user_count',
    'before_usertalk_count',
    'after_article_sizediff',
    'after_unique_articles',
    'after_article_count',
    'after_edit_count',
    'after_talk_count',
    'after_user_count',
    'after_usertalk_count']
    
    bf_data = bf_data[columns]


    return (bf_data)


