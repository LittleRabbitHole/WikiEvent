# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 09:52:07 2016
Last updated: 2/12/2017

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

#read username file
data = pd.read_csv("newcomers_for_talkpage.csv", encoding = "ISO-8859-1")
data['register'] = data['register'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%y').strftime('%Y-%m-%d'))
data['onemonth'] = data['onemonth'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%y').strftime('%Y-%m-%d'))

data.columns.values
list_allwpids = list(data["wpid"])
len(list_allwpids) #545

####----------
##############################################################################
## add source and target to seperate
#retrive only the edits before the end of the class
incoming = pd.read_csv("students_talk_2015-2016_0522.csv", encoding = "ISO-8859-1")
incoming['source'] = incoming['talkpage_editor']
incoming['target'] = incoming['wpid']

outgoing = pd.read_csv("StudentContri_talkout_Wiki__2015-2016_0522.csv", encoding = "ISO-8859-1")
outgoing['source'] = outgoing['student_wpid']
outgoing['target'] = outgoing['talkpage_owner']


len(incoming) #35009
len(incoming["wpid"].drop_duplicates()) #14337 unique user edited in the talk page
len(outgoing["target"].drop_duplicates()) #746 wikipedians 
#user_talk = user_talk.drop_duplicates()
#user_talk.to_csv("user_talk_v2.csv",index=False)



################
#user_talk add classmates
#read username file
user_talk = incoming
user_talk = outgoing
user_talk.columns.values

source_type = []
for index, row in user_talk.iterrows():
    if row['source'] == row['target']:
        source_type.append(1) #self
    elif row['source'] in studentList:
        source_type.append(2) #classmates, in student list
    else:
        source_type.append(0) #wikipedian
        
source_type_se = pd.Series(source_type)
user_talk['source_type'] = source_type_se.values


target_type = []
for index, row in user_talk.iterrows():
    if row['target'] == row['source']:
        target_type.append(1) #self
    elif row['target'] in studentList:
        target_type.append(2) #classmates, in student list
    else:
        target_type.append(0) #wikipedian
        
target_type_se = pd.Series(target_type)
user_talk['target_type'] = target_type_se.values #self, owner of talk page



##############################
###adding edit interval
#encoding time stamp
user_talk["timestamp"]=user_talk['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y %H:%M:%S'))
#change to timestamp for furture process
user_talk['timestamp'] = pd.to_datetime(user_talk['timestamp'])

#calculate the response time based on the 
user_talk['Response(Sec)'] = None
user_talk['EditDelta(Sec)'] = None

''''
#this part useless
for i in range(1,len(user_talk['timestamp'])):
    diff_inSeconds = (user_talk['timestamp'][i] - user_talk['timestamp'][i-1]).total_seconds()
    if user_talk['wpid'][i] == user_talk['wpid'][i-1]:
        user_talk.set_value(i, 'EditDelta(Sec)', diff_inSeconds)
    else:  
        user_talk.set_value(i, 'EditDelta(Sec)',None)
'''

for i in range(1,len(user_talk['timestamp'])):
    diff_inSeconds = (user_talk['timestamp'][i] - user_talk['timestamp'][i-1]).total_seconds()
    if user_talk['target'][i] == user_talk['target'][i-1] and user_talk['source'][i] == user_talk['source'][i-1]:
        user_talk.set_value(i, 'Response(Sec)', diff_inSeconds)
        #user_talk.set_value(i, 'Response(time)',str(datetime.timedelta(seconds=diff_inSeconds))) in mins
    else:  
        user_talk.set_value(i, 'Response(Sec)', None)


user_talk.to_csv("student_talkpage_2015-2016_relationship_0306.csv",index=False)
user_talk.to_csv("wikipeidan_talkpage_2015-2016_relationship_0306.csv",index=False)


########################
# relationship network data preperation
user_talk_relation = pd.read_csv("student_talkpage_2015-2016_relationship_0522_session12.csv", encoding = "ISO-8859-1")
user_talk_relation = pd.read_csv("wikipeidan_talkpage_2015-2016_relationship_0522_session12.csv", encoding = "ISO-8859-1")

user_talk_relation = pd.read_csv("student_talkpage_2015-2016_relationship_0522_session3.csv", encoding = "ISO-8859-1")
user_talk_relation = pd.read_csv("wikipeidan_talkpage_2015-2016_relationship_0522_session3.csv", encoding = "ISO-8859-1")

user_talk_relation.columns.values

aggre_data={}
#group by wpid
#Grouped = artical_talk_relation.groupby(['wpid'])
Grouped = user_talk_relation.groupby(['target'])

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
df_Contact.reset_index(level=1, inplace=True)
df_Contact.reset_index(level=0, inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'source'})
df_Contact = df_Contact.rename(columns={'index': 'target'})
        
df_Contact.columns.values

#######---------------------------------------
df_Contact.to_csv("student_talkpage_2015-2016_network_session12_0524.csv", index=False)

#######---------------------------------------
df_Contact.to_csv("wikipedian_talkpage_2015-2016_network_session12_0524.csv", index=False)

#################################################
#conbine two dataset together wikipedian_talk_network_0210 and student_talk_network_0210
wikipedian_talk = pd.read_csv("wikipedian_talkpage_2015-2016_network_session1_0522.csv", encoding = "ISO-8859-1")#student_talk_network_0210
student_talk = pd.read_csv("student_talkpage_2015-2016_network_session1_0522.csv", encoding = "ISO-8859-1")

wikipedian_talk = pd.read_csv("wikipedian_talkpage_2015-2016_network_session2_0522.csv", encoding = "ISO-8859-1")#student_talk_network_0210
student_talk = pd.read_csv("student_talkpage_2015-2016_network_session2_0522.csv", encoding = "ISO-8859-1")

wikipedian_talk = pd.read_csv("wikipedian_talkpage_2015-2016_network_session3_0522.csv", encoding = "ISO-8859-1")#student_talk_network_0210
student_talk = pd.read_csv("student_talkpage_2015-2016_network_session3_0522.csv", encoding = "ISO-8859-1")

wikipedian_talk = pd.read_csv("wikipedian_talkpage_2015-2016_network_session12_0524.csv", encoding = "ISO-8859-1")#student_talk_network_0210
student_talk = pd.read_csv("student_talkpage_2015-2016_network_session12_0524.csv", encoding = "ISO-8859-1")


frames = [student_talk, wikipedian_talk]
result = pd.concat(frames)
result.columns.values

result.to_csv("Talk_network_2015-2016_session1_0522.csv", index=False)
result.to_csv("Talk_network_2015-2016_session2_0522.csv", index=False)
result.to_csv("Talk_network_2015-2016_session3_0522.csv", index=False)
result.to_csv("Talk_network_2015-2016_session12_0524.csv", index=False)

#######---------------------------------------
###############
user_talk_network = pd.read_csv("Talk_network_2015-2016_session1_0522.csv", encoding = "ISO-8859-1")
user_talk_network = pd.read_csv("Talk_network_2015-2016_session2_0522.csv", encoding = "ISO-8859-1")
user_talk_network = pd.read_csv("Talk_network_2015-2016_session3_0522.csv", encoding = "ISO-8859-1")

user_talk_network = pd.read_csv("Talk_network_2015-2016_session12_0524.csv", encoding = "ISO-8859-1")

talk_network = user_talk_network

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


[FinalID_gphy, AllNetwork_gphy] = writeout(user_talk_network)




#write out
FinalID_gphy.to_csv("session1_2015-2016_ID_gphy_0522.csv", index=False)
AllNetwork_gphy.to_csv("session1_Network_2015-2016_gphy_0522.csv", index=False)

FinalID_gphy.to_csv("session2_2015-2016_ID_gphy_0522.csv", index=False)
AllNetwork_gphy.to_csv("session2_Network_2015-2016_gphy_0522.csv", index=False)

FinalID_gphy.to_csv("session3_2015-2016_ID_gphy_0522.csv", index=False)
AllNetwork_gphy.to_csv("session3_Network_2015-2016_gphy_0522.csv", index=False)


AllNetwork_gphy.to_csv("session12_Network_2015-2016_0524.csv", index=False)

########################################################
def writeout_Committed(talk_network):
    "input network data, output csv prepare for Gphy with ID, and edges"
    egoID = talk_network["wpid"].tolist() #keep all ego id from full network
    #len(set(egoID))103
    ego_type = talk_network["wpid_type"].tolist() 

    otherID = talk_network["user"].tolist()
    other_type = talk_network["user_type"].tolist()
    
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

    #create dataframe for this
    Final_ID = pd.DataFrame({"wpid": IDs, "user_type": user_type})
    Final_ID = Final_ID.drop_duplicates(subset=['wpid', 'user_type'])   
    #len(Final_ID["user_type"][Final_ID["user_type"]==1]) #103
    ID_list = pd.Series(list(range(len(Final_ID)+1))[1::])
    Final_ID["ID"] = ID_list.values

    #source target ID
    Final_Network =  pd.merge(talk_network, Final_ID[["ID","wpid"]], left_on="wpid", right_on="wpid",how='left', sort=False)
    Final_Network = Final_Network.rename(columns={"ID":"Target"})
    
    Final_Network =  pd.merge(Final_Network, Final_ID[["ID","wpid"]], left_on="user", right_on="wpid",how='left', sort=False)
    Final_Network = Final_Network.drop('wpid_y', 1)
    Final_Network = Final_Network.rename(columns={"ID":"Source"})
    return Final_ID, Final_Network

commitedNetwork = pd.read_csv("commited_user_talk_network_0212.csv")
leftNetwork = pd.read_csv("left_user_talk_network_0212.csv")

[commitedID_gphy, commitedNetwork_gphy] = writeout_Committed(commitedNetwork)
[leftID_gphy, leftNetwork_gphy] = writeout_Committed(leftNetwork)

commitedID_gphy.to_csv("commited_ID_gphy_0213.csv", index=False)
commitedNetwork_gphy.to_csv("commitedNetwork_gphy_0213.csv", index=False)

leftID_gphy.to_csv("left_ID_gphy_0213.csv", index=False)
leftNetwork_gphy.to_csv("leftNetwork_gphy_0213.csv", index=False)

#------separate into classmates and wikipedians
#commitedNetwork = pd.read_csv("commited_user_talk_network_0203.csv")
#talk_network = commitedNetwork
#Class_commitednetwork = commitedNetwork[commitedNetwork["user_type"]==2]
#separated_network = Class_commitednetwork
#wiki_commitednetwork = commitedNetwork[commitedNetwork["user_type"]==0]

def writeout_seperate(talk_network, separated_network):
    "input network data, output csv prepare for Gphy with ID, and edges"
    egoID = talk_network["wpid"].tolist() #keep all ego id from full network
    #len(set(egoID))103
    ego_type = talk_network["wpid_type"].tolist() 

    otherID = separated_network["user"].tolist()
    other_type = separated_network["user_type"].tolist()
    
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

    #create dataframe for this
    Final_ID = pd.DataFrame({"wpid": IDs, "user_type": user_type})
    Final_ID = Final_ID.drop_duplicates(subset=['wpid', 'user_type'])   
    #len(Final_ID["user_type"][Final_ID["user_type"]==1]) #103
    ID_list = pd.Series(list(range(len(Final_ID)+1))[1::])
    Final_ID["ID"] = ID_list.values

    #source target ID
    Final_Network =  pd.merge(separated_network, Final_ID[["ID","wpid"]], left_on="wpid", right_on="wpid",how='left', sort=False)
    Final_Network = Final_Network.rename(columns={"ID":"Target"})
    
    Final_Network =  pd.merge(Final_Network, Final_ID[["ID","wpid"]], left_on="user", right_on="wpid",how='left', sort=False)
    Final_Network = Final_Network.drop('wpid_y', 1)
    Final_Network = Final_Network.rename(columns={"ID":"Source"})
    return Final_ID, Final_Network

Class_commitednetwork = commitedNetwork[commitedNetwork["user_type"]==2]
wiki_commitednetwork = commitedNetwork[commitedNetwork["user_type"]==0]
[Class_CommitedID_gphy, Class_commitedNetwork_gphy] = writeout_seperate(commitedNetwork, Class_commitednetwork)
[wiki_CommitedID_gphy, wiki_commitedNetwork_gphy] = writeout_seperate(commitedNetwork, wiki_commitednetwork)

#write out
Class_CommitedID_gphy.to_csv("Class_CommitedID_gphy_0203.csv", index=False)
Class_commitedNetwork_gphy.to_csv("Class_commitedNetwork_0203.csv", index=False)
wiki_CommitedID_gphy.to_csv("wiki_CommitedID_gphy_0203.csv", index=False)
wiki_commitedNetwork_gphy.to_csv("wiki_commitedNetwork_0203.csv", index=False)


Class_leftnetwork = leftNetwork[leftNetwork["user_type"]==2]
wiki_leftnetwork = leftNetwork[leftNetwork["user_type"]==0]
[Class_leftID_gphy, Class_leftNetwork_gphy] = writeout_seperate(leftNetwork, Class_leftnetwork)
[wiki_leftID_gphy, wiki_leftNetwork_gphy] = writeout_seperate(leftNetwork, wiki_leftnetwork)

#write out
Class_leftID_gphy.to_csv("Class_leftID_gphy_0203.csv", index=False)
Class_leftNetwork_gphy.to_csv("Class_leftNetwork_0203.csv", index=False)
wiki_leftID_gphy.to_csv("wiki_leftID_gphy_0203.csv", index=False)
wiki_leftNetwork_gphy.to_csv("wiki_leftNetwork_0203.csv", index=False)

#--------------Test----------------------------
commitedNetwork = pd.read_csv("left_user_talk_network_0203.csv")

egoID = commitedNetwork["wpid"].tolist()
#len(set(egoID))103 1741
ego_type = len(egoID)*[1] #self is 1

otherID = commitedNetwork["user"].tolist()
other_type = commitedNetwork["user_type"].tolist()

#apprear in the ego als appear in the other
a = list(set(egoID))
len(list(set(egoID) & set(otherID)))

#remove other ids appear in the ego
n=len(otherID)
for i in range(n):
    if otherID[i] in egoID:
        otherID[i] = "Need_del"
        other_type[i] = "Need_del"

otherID = [x for x in otherID if x != "Need_del"]
other_type = [x for x in other_type if x != "Need_del"]


IDs = egoID + otherID
user_type = ego_type + other_type

#create dataframe for this
CommitedID = pd.DataFrame({"wpid": IDs, "user_type": user_type})
CommitedID = CommitedID.drop_duplicates(subset=['wpid', 'user_type'])   
#len(CommitedID["user_type"][CommitedID["user_type"]==1]) #103
ID_list = pd.Series(list(range(len(CommitedID)+1))[1::])
CommitedID["ID"] = ID_list.values

#source target ID
commitedNetwork =  pd.merge(commitedNetwork, CommitedID[["ID","wpid"]], left_on="wpid", right_on="wpid",how='left', sort=False)
commitedNetwork = commitedNetwork.rename(columns={"ID":"Target"})

commitedNetwork =  pd.merge(commitedNetwork, CommitedID[["ID","wpid"]], left_on="user", right_on="wpid",how='left', sort=False)
commitedNetwork = commitedNetwork.drop('wpid_y', 1)
commitedNetwork = commitedNetwork.rename(columns={"ID":"Source"})







