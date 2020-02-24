# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 09:52:07 2016
Last updated: 4/19/2018

this includes the steps from incoming/outgoing talk page contribution data collected from API to generate the talk net

@author: angli
"""
import datetime
import urllib.parse
import json
import csv
import os
import pandas as pd
import numpy as np

# read the list of users
os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")

def isOneMonth(c):
    if c['time_index'] <= 7: 
        return (1)
    else:
        return (0)

#input contribution data
#return time truncated data
def time_Truncate(talk_data):
    
    newcomerstime = pd.read_table("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data/newcomers_for_talkpage_v2.txt", sep = "\t")
    merge_time = pd.merge(talk_data, newcomerstime, how='left', on=['wpid', 'userid', 'event'])
    #merge_time.columns.values
    #merge_time["timestamp"]=merge_time['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y %H:%M:%S'))
    merge_time['current_date'] = merge_time['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
    merge_time['current_date'] = pd.to_datetime(merge_time['current_date'])
    merge_time['register'] = pd.to_datetime(merge_time['register'])
    merge_time['time_index'] = (merge_time['current_date'] - merge_time['register'])/np.timedelta64(1, 'D')
    merge_time["Early_experience"] = merge_time.apply(isOneMonth, axis=1)
    final_data = merge_time.loc[merge_time['Early_experience'] == 1] #to get only the one week data
    #final_data = merge_time
    return (final_data)

def checkBalance(contri_data):
    print ("total: ", len(set(list(contri_data["userid"]))))
    print ("early: ", len(set(list(contri_data["userid"].loc[contri_data['Early_experience'] == 1]))))
    #print ("after: ", len(set(list(contri_data["userid"].loc[contri_data['Early_experience'] == 0]))))



#read username file
incoming = pd.read_csv("talk_page_1month_incoming.csv", encoding = "ISO-8859-1")
oneweek_income = time_Truncate(incoming)

outgoing = pd.read_csv("outgoing_1month.csv", encoding = "ISO-8859-1")
oneweek_outgoing = time_Truncate(outgoing)


checkBalance(oneweek_outgoing) #545

####----------
##############################################################################
## add source and target to seperate
# remove bot message
incoming = oneweek_income
incoming = incoming[incoming["messagein"].str.contains('bot|Bot|BOT')==False]
incoming['source'] = incoming['messagein']
incoming['target'] = incoming['wpid']
incoming.columns.values
incoming = incoming[['wpid', 'userid', 'timestamp', 'size', 'source', 'target']]

outgoing = oneweek_outgoing
outgoing = outgoing[outgoing["message_to"].str.contains('bot|Bot|BOT')==False]
outgoing['source'] = outgoing['wpid']
outgoing['target'] = outgoing['message_to']
outgoing.columns.values
outgoing = outgoing[['wpid', 'userid', 'timestamp', 'size', 'source', 'target']]

talkdata = pd.concat([incoming, outgoing])

#talkdata["self"] = 0
#talkdata.loc[(talkdata['source'] == talkdata['target'] ),'self'] = 1
#
#talkdata = talkdata.loc[talkdata['self']==0]

################
#add user type
users = pd.read_csv("Newcomers_groups.csv", encoding = "ISO-8859-1")
users = users[["wpid", "first_edit_type3"]]
users_dict = dict(zip(users.wpid, users.first_edit_type3))

#read username file

source_type = []
target_type = []
first_edit_type3 = []
for index, row in talkdata.iterrows():
    wpid = row['wpid']
    user_type = users_dict.get(wpid, -1)
    first_edit_type3.append(user_type)
    sourceid = row['source']
    source_usertype = users_dict.get(sourceid, -1) #-1 wikipedians
    source_type.append(source_usertype) 
    targetid = row['target']
    target_usertype = users_dict.get(targetid, -1)
    target_type.append(target_usertype) 
        
talkdata['source_type'] = source_type
talkdata['target_type'] = target_type
talkdata['first_edit_type3'] = first_edit_type3

talkdata = talkdata.loc[talkdata['first_edit_type3'] != -1]

#aggregation
aggre_data={}
Grouped = talkdata.groupby(['target'])

#n=0
for pidgroup in Grouped:
#    n+=1
#    if n==5: break
    target = pidgroup[0]
    #user_editDelta = np.mean(pidgroup[1]["EditDelta(Sec)"])
    aggre_data[target]={}
    user_grouped = pidgroup[1].groupby(['source'])
#    m=0
    for usergroup in user_grouped:
#        m+=1
#        if m==1: break
        source = usergroup[0]
        aggre_data[target][source]={}
        #target type
        target_type = list(usergroup[1]["target_type"])[0]
        aggre_data[target][source]['target_type'] = target_type
        # source type count
        source_type = list(usergroup[1]["source_type"])[0]
        user_revision_times = len(list(usergroup[1]["source_type"]))
        #user_ResponseDelta = np.mean(usergroup[1]["Response(Sec)"])
        user_EditSize = np.mean(usergroup[1]["size"])
        #aggre_data[wid][user]["wid_PgEditedIntervel"] = user_editDelta  
        aggre_data[target][source]['user_EditSize'] = user_EditSize
        aggre_data[target][source]['source_type'] = source_type
        aggre_data[target][source]['user_revision_times'] = user_revision_times
        #aggre_data[target][source]['user_responseEditInterval'] = user_ResponseDelta

#convert aggre_data dictionary into dataframe
#convert ContactAve dictionary into dataframe        
names = []
frames = []
for name, d in aggre_data.items():    
    names.append(name)
    frames.append(pd.DataFrame.from_dict(d, orient='index'))
            
df_Contact = pd.concat(frames, keys=names).sort_index()

#drop index into columns
df_Contact.reset_index(inplace=True)
#df_Contact.reset_index(level=0, inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'source'})
df_Contact = df_Contact.rename(columns={'level_0': 'target'})
        
df_Contact.columns.values

#######---------------------------------------
df_Contact.to_csv("talknetwork.csv", index=False)



#######---------------------------------------
###############

talk_network = df_Contact

def writeout(talk_network):
    "input network data, output csv prepare for Gphy with ID, and edges"
    egoID = talk_network["source"].tolist()
    #len(set(egoID))103
    ego_type = talk_network["source_type"].tolist() 

    otherID = talk_network["target"].tolist()
    other_type = talk_network["target_type"].tolist()
    
    #apprear in the ego als appear in the other
    #list(set(egoID) & set(otherID))

    #remove other ids appear in the ego
    for i in range(len(otherID)):
        if otherID[i] in egoID:
            otherID[i] = "Need_del"
            other_type[i] = "Need_del"

    otherID = [x for x in otherID if x != "Need_del"]
    other_type = [x for x in other_type if x != "Need_del"]
            
    IDs = egoID + otherID
    user_type = ego_type + other_type
    #commited = talk_network["commited"].tolist()

    #create dataframe for this
    Final_ID = pd.DataFrame({"wpid": IDs, "user_type": user_type})
    Final_ID = Final_ID.drop_duplicates(subset=['wpid', 'user_type'])   
    #len(Final_ID["user_type"][Final_ID["user_type"]==1]) #103
    ID_list = pd.Series(list(range(len(Final_ID)+1))[1::])
    Final_ID["ID"] = ID_list.values

    #source target ID
    Final_Network =  pd.merge(talk_network, Final_ID[["ID","wpid"]], left_on="target", right_on="wpid",how='left', sort=False)
    Final_Network = Final_Network.rename(columns={"ID":"Target_ID"})
    
    Final_Network =  pd.merge(Final_Network, Final_ID[["ID","wpid"]], left_on="source", right_on="wpid",how='left', sort=False)
    Final_Network = Final_Network.drop('wpid_y', 1)
    Final_Network = Final_Network.rename(columns={"ID":"Source_ID"})
    #Commited_Network = Final_Network[Final_Network["commited"] == 1]
    #left_Network = Final_Network[Final_Network["commited"] == 0]
    return Final_ID, Final_Network


[FinalID_gphy, AllNetwork_gphy] = writeout(talk_network)




#write out
FinalID_gphy.to_csv("takepage_ids_1w.csv", index=False)
AllNetwork_gphy.to_csv("takepage_edges_1w.csv", index=False)





