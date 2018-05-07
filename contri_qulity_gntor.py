#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 10:21:11 2018

this is to prepare the aggregated data for 4 month contribution quality of article pages

@author: angli
"""
import pandas as pd
import numpy as np
import os
import datetime

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")

def quality_aggre_sep(data):
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
        #userid
        userid = list(piddata["userid"])[0]
        aggre_data[wid]['userid'] = userid
        #event
        event = list(piddata["event"])[0]
        aggre_data[wid]['event'] = event
        
        #before data
        #before = piddata.loc[piddata['after_first_event_edit'] == 0]
        before = piddata.loc[piddata['Early_experience'] == 1]
    
        #total edit count
        before_edit_count = len(list(before['wpid']))
        aggre_data[wid]['before_edit_count'] = before_edit_count
        
        try: 
            before_ave_true = sum(list(before['prob-true']))/(len(list(before['prob-true'])))
        except ZeroDivisionError:
            before_ave_true = None
        aggre_data[wid]['before_ave_true'] = before_ave_true
        
        #after data
        #after = piddata.loc[piddata['after_first_event_edit'] == 1]
        after = piddata.loc[piddata['Early_experience'] == 0]
    
        #total edit count
        after_edit_count = len(list(after['wpid']))
        aggre_data[wid]['after_edit_count'] = after_edit_count

        #ave true
        try:
            after_ave_true = sum(list(after['prob-true']))/(len(list(after['prob-true'])))
        except ZeroDivisionError:
            after_ave_true = None
        
        aggre_data[wid]['after_ave_true'] = after_ave_true
       
        if n%100==0: print (n)
    
    bf_data = pd.DataFrame.from_dict(aggre_data, orient='index')
    #change columns order
    qual_columns = ["wpid", 'userid','event','first_edit_type3',
                'before_edit_count',
               'before_ave_true', 
               'after_ave_true']
   
    bf_data = bf_data[qual_columns]

    return (bf_data)


def revert_aggre_sep(data):
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
        #userid
        userid = list(piddata["userid"])[0]
        aggre_data[wid]['userid'] = userid
        #event
        event = list(piddata["event"])[0]
        aggre_data[wid]['event'] = event
        
        #before data
        #before = piddata.loc[piddata['after_first_event_edit'] == 0]
        before = piddata.loc[piddata['Early_experience'] == 1]
    
        #total edit count
        before_edit_count = len(list(before['wpid']))
        aggre_data[wid]['before_edit_count'] = before_edit_count
                
        #revert
        before_revert_list = list(before['reverted'])
        before_no_revert_count = before_revert_list.count(0) 
        before_revert_count = before_revert_list.count(1)
        aggre_data[wid]['before_revert_count'] = before_revert_count
        
        try:
            before_revert_ratio = before_revert_count/(before_revert_count + before_no_revert_count)
        except ZeroDivisionError:
            before_revert_ratio = None
        aggre_data[wid]['before_revert_ratio'] = before_revert_ratio
        
        #after data
        #after = piddata.loc[piddata['after_first_event_edit'] == 1]
        after = piddata.loc[piddata['Early_experience'] == 0]
    
        #total edit count
        after_edit_count = len(list(after['wpid']))
        aggre_data[wid]['after_edit_count'] = after_edit_count

#         #ave true
#        after_ave_true = sum(list(after['prob-true']))/(len(list(after['prob-true'])) + 0.0001)
#        aggre_data[wid]['after_ave_true'] = after_ave_true
        
        #revert
        after_revert_list = list(after['reverted'])
        after_no_revert_count = after_revert_list.count(0) 
        after_revert_count = after_revert_list.count(1)
        aggre_data[wid]['after_revert_count'] = after_revert_count
        
        try:
            after_revert_ratio = after_revert_count/(after_revert_count + after_no_revert_count)
        except ZeroDivisionError:
            after_revert_ratio = None        
        aggre_data[wid]['after_revert_ratio'] = after_revert_ratio
        
        if n%100==0: print (n)
    
    bf_data = pd.DataFrame.from_dict(aggre_data, orient='index')
    #change columns order
#    qual_columns = ["wpid", 'userid','event','first_edit_type3',
#               'before_ave_true', 
#               'after_ave_true']

    revert_columns = ["wpid", 'userid','event','first_edit_type3',
               'before_revert_count','before_revert_ratio',
               'after_revert_count', 'after_revert_ratio']
    
    bf_data = bf_data[revert_columns]


    return (bf_data)



def isOneMonth(c):
    if c['time_index'] <= 7: 
        return (1)
    else:
        return (0)

#input contribution data
#return time truncated data
def time_Truncate(contri_data):
    
    newcomerstime = pd.read_table("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/newcomers_for_talkpage_v2.txt", sep = "\t")
    merge_time = pd.merge(contri_data, newcomerstime, how='left', on=['wpid', 'userid', 'event'])
    #merge_time.columns.values
    #merge_time["timestamp"]=merge_time['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y %H:%M:%S'))
    merge_time['current_date'] = merge_time['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
    merge_time['current_date'] = pd.to_datetime(merge_time['current_date'])
    merge_time['register'] = pd.to_datetime(merge_time['register'])
    merge_time['time_index'] = (merge_time['current_date'] - merge_time['register'])/np.timedelta64(1, 'D')
    merge_time["Early_experience"] = merge_time.apply(isOneMonth, axis=1)
    #final_data = merge_time.loc[merge_time['time_marker_1m'] == 1] #to get only the one month later data
    final_data = merge_time
    return (final_data)

def checkBalance(contri_data):
    print ("total: ", len(set(list(contri_data["userid"]))))
    print ("early: ", len(set(list(contri_data["userid"].loc[contri_data['Early_experience'] == 1]))))
    print ("after: ", len(set(list(contri_data["userid"].loc[contri_data['Early_experience'] == 0]))))

#checkBalance(merge_time)

if __name__ == "__main__":
    
    users = pd.read_csv("Newcomers_groups.csv", encoding = "ISO-8859-1")
    users.event = users.event.str.lower()
    #contribution data
    contri_data = pd.read_csv("newcomers_contri_quality.csv", encoding = "ISO-8859-1") 
    contri_data.event = contri_data.event.str.lower()
    contri_data = pd.merge(contri_data, users, on=['wpid', 'userid', 'event'])
    #len(set(list(contri_data["userid"])))
    
    contri_data = time_Truncate(contri_data)
    
    #quality aggregation
    aggre_quality = quality_aggre_sep(contri_data)
    
    #revert
    revert_data = pd.read_csv("newcomers_contri_revert.csv", encoding = "ISO-8859-1")
    revert_data.event = revert_data.event.str.lower()
    revert_data = pd.merge(revert_data, users, on=['wpid', 'userid', 'event'])
    #len(set(list(revert_data["userid"])))
    revert_data = time_Truncate(revert_data)
    #revert aggregation
    aggre_revert = revert_aggre_sep(revert_data)
    
    revert_quality_aggre = pd.merge(aggre_quality, aggre_revert, on=['wpid', 'userid', 'event', 'first_edit_type3'])
    
    revert_quality_aggre.to_csv("qual_factors_outcomes.csv", index=False)
