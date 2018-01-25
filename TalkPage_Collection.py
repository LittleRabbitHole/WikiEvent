# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 10:52:16 2017
last update: 03/06
This is to collect the user talk page data for year 2015-2016 period
@author: angli

Data is collected following the steps"
1. collect all student talkpage edit history, including both with students and with wikipedians
This step collacted all the students edit in and out, wikipedians edited in students' talk page
2. collect all students edit out to wikipeidans' talk page
This step collacted all the students edit out to wikipedians' talk page

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

data.to_csv("newcomers_for_talkpage_v2.txt", sep = '\t',index=False)
#########################################
###edit in to students' user talk page
csv_f = csv.writer(open("talk_page_1month.csv", "w", encoding="UTF-8"))
#write first row

csv_f.writerow(['wpid','userid','usertype','event',
                'article','ns','page_title','messagein', 'timestamp', 'size','comment'])
#write error
csv_error = csv.writer(open("talk_page_errorusers.csv", "w", encoding="UTF-8"))


#read coursematch file
fo = open("newcomers_for_talkpage_v2.txt")
info=fo.readlines()
fo.close()

# read username catch the Wiki data
n=0
for line in info[1::]:
    line = line.strip()
    info_lst = line.split('\t')
    #userinfo
    userid = info_lst[1]
    event = info_lst[-1]
    #usertype
    usertype = info_lst[4]
    #get name from names list
    wpid = info_lst[0]
    wpid = wpid.strip()
    #print (rawID)
    wpid_nospace = wpid.replace(" ","_")
    #decode student's name into ascii
    decode_wpid = urllib.parse.quote(wpid_nospace)
    #onemonth time window
    starttime = info_lst[2]+"T00:00:00Z"
    endtime = info_lst[3]+"T11:59:59Z"
    #api
    #api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuser={}&ucdir=newer&ucstart={}&ucend={}&ucprop=title|timestamp|comment|size|sizediff|flags&uclimit=500&format=json").format(decode_wpid,starttime,endtime)#Kingsleyta
    #https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=User_talk:Kingsleyta&rvdir=newer&rvend=2015-03-30T13:20:52Z&rvprop=timestamp|user|size|comment&rvdir=newer&rvlimit=500    
    api_call = ("https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=User_talk:{}&rvdir=newer&rvstart={}&rvend={}&rvprop=timestamp|user|size|comment&rvdir=newer&rvlimit=500&format=json").format(decode_wpid,starttime,endtime)#Kingsleyta
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    page_id = list(responsedata["query"]["pages"].keys())[0]
    page_title = responsedata['query']['pages'][page_id].get("title")
    try: 
        revision_data_lst=responsedata['query']['pages'][page_id]['revisions']
        for feature in revision_data_lst:
            if feature.get('user') != wpid: #not writeout self editing
                csv_f.writerow([ wpid, userid,usertype,event,"user_talk", 3, page_title, feature.get('user'), feature['timestamp'], feature.get('size')])
    except KeyError:
        csv_error.writerow([wpid, userid,usertype,event,"user_talk", 3, page_title, "no user talk", "no user talk","no user talk","no user talk"])
    n+=1
    if n%100==0:
        print (n) 



##########################################
### collect student contribution in user talk page (edit out)

csv_f = csv.writer(open("outgoing_1month.csv", "w"))
#write first row
csv_f.writerow(['wpid','userid','usertype','event',
                'article','ns','page_title','message_to', 'timestamp', 'size','comment'])
#write error
csv_error = csv.writer(open("outgoing_errorusers_1month.csv", "w", encoding="UTF-8"))

#user talk page revisions
#regular expression to match out the username
import re
talkpageowner_re = re.compile(r'User talk:(.*)/|User talk:(.*)', re.DOTALL)#User talk:(.*)/sandbox|User talk:(.*)/

n=0
for line in info[1::]:
    line = line.strip()
    info_lst = line.split('\t')
    #userinfo
    userid = info_lst[1]
    event = info_lst[-1]
    #usertype
    usertype = info_lst[4]
    #get name from names list
    wpid = info_lst[0]
    wpid = wpid.strip()
    #print (rawID)
    wpid_nospace = wpid.replace(" ","_")
    #decode student's name into ascii
    decode_wpid = urllib.parse.quote(wpid_nospace)
    #onemonth time window
    starttime = info_lst[2]+"T00:00:00Z"
    endtime = info_lst[3]+"T11:59:59Z"
    
    #API https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=User_talk:Averysf&rvuser=Poetries&rvdir=newer&rvprop=timestamp|user|size|comment&rvdir=newer&rvlimit=500
    api_call = ("https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuser={}&ucnamespace=3&ucdir=newer&ucstart={}&ucend={}&ucprop=timestamp|title|sizediff|comment&uclimit=500&format=json").format(decode_wpid,starttime,endtime)#Kingsleyta
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    try: 
        revision_data_lst=responsedata["query"]["usercontribs"]#list
        for feature in revision_data_lst:
            talk_page_title = feature.get('title')
            talk_page_owner1 = re.match(talkpageowner_re, talk_page_title).group(1)
            talk_page_owner2 = re.match(talkpageowner_re, talk_page_title).group(2)
            talk_page_owner_lst=list(set([talk_page_owner1,talk_page_owner2]))
            talk_page_owner = [x for x in talk_page_owner_lst if x is not None][0]
            if wpid != talk_page_owner: #not edit in their own talk page
                csv_f.writerow([wpid, userid,usertype,event,"user_talk", 3, talk_page_title, talk_page_owner, feature['timestamp'], feature.get('sizediff')])
    except KeyError:
        #pass
        csv_error.writerow([wpid, userid,usertype,event,"user_talk", 3, page_title, "no user talk", "no user talk","no user talk","no user talk"])
    n+=1    
    if n%100==0:
        print (n) 

#https://en.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuser=Kingsleyta&ucdir=newer&ucnamespace=3&ucprop=title|timestamp|comment|size|sizediff|flags&uclimit=500
###########################################################
###########################################################
#prepare network
user_talk = pd.read_csv("talkpage_all_edits.csv", encoding = "ISO-8859-1")

user_talk.columns.values

aggre_data={}
#group by wpid
#Grouped = artical_talk_relation.groupby(['wpid'])
Grouped = user_talk.groupby(['target'])

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
        # source type count
        source_data = list(usergroup[1]["source"])[0]
        user_revision_times = len(list(usergroup[1]["source"]))
        aggre_data[target][source]['revision_times'] = user_revision_times
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
df_Contact.reset_index(inplace=True)
#rename index column
df_Contact = df_Contact.rename(columns={'level_1': 'source'})
df_Contact = df_Contact.rename(columns={'index': 'target'})
        
df_Contact.columns.values

#######---------------------------------------
df_Contact.to_csv("talk_net.csv", index=False)





##useless
###########################################################
###########################################################

#####after manually remove sandbox title
Contri_talk = pd.read_csv("studentContri_talkout_2015-2016_0306.csv", encoding = "ISO-8859-1")
Contri_talk.columns.values
drop_index = []
for index, row in studentContri_talk.iterrows():
    if row['wpid'] == row['talkpage_owner']:
        drop_index.append(index)

#drop self editing    
studentContri_talk = studentContri_talk.drop(studentContri_talk.index[drop_index])

#reset index
studentContri_talk = studentContri_talk.reset_index(drop=True)

#drop edit classmates
#leave only wikipedian
drop_index = []
for index, row in studentContri_talk.iterrows():
    if row['talkpage_owner'] in studentList:
        drop_index.append(index)

studentContri_talk = studentContri_talk.drop(studentContri_talk.index[drop_index])
studentContri_talk = studentContri_talk.reset_index(drop=True)

studentContri_talk.to_csv("StudentContri_talkout_Wiki_2015-2016_0306.csv",index=False)

#wikipedians students talked to
wiki_list = list(set(list(studentContri_talk['talkpage_owner'])))
len(wiki_list) #740 student contribute to 740 wikipedians' talk page






##useless
###########################################################
#during semester
students_talk = pd.read_csv("students_talk_2015-2016_0306.csv", encoding = "ISO-8859-1")
students_talk.columns.values

#wikipedians talked to
talk_pair = students_talk[["wpid","talkpage_user"]]#.drop_duplicates()
students_rowindex = []
for i, row in talk_pair.iterrows():
    if row["talkpage_user"] in studentList:
        students_rowindex.append(i)
        
talk_pair = talk_pair.drop(talk_pair.index[students_rowindex])  
talk_pair = talk_pair.drop_duplicates() #talk pair of student and wikipedians     

wiki_list = talk_pair["talkpage_user"].tolist() #Wikipedian list student talked to
user_list = talk_pair["wpid"].tolist() #corresponded student list

#collect the wikipedian talk page edited by students
#Check the user talk page
csv_f = csv.writer(open("wiki_talk_2015-2016_0306.csv", "w"))
#write first row
csv_f.writerow(['wpid_original', 'wikipedian','article','ns','page_title','students', 'timestamp', 'size','comment'])

#user talk page revisions

n=0
for i in range(len(wiki_list)):
    wpid_original =  wiki_list[i]
    sdutent_original = user_list[i]
    #Wikipedians
    wpid = wpid_original.strip()
    wpid_nospace = wpid.replace(" ","_")
    #article = articles[i]
    #decode student's name into ascii
    decode_wpid = urllib.parse.quote(wpid_nospace)
    #Students
    studentid = sdutent_original.strip()
    sid_nospace = studentid .replace(" ","_")
    #article = articles[i]
    #decode student's name into ascii
    decode_sid = urllib.parse.quote(sid_nospace)
    #API https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=User_talk:Averysf&rvuser=Poetries&rvdir=newer&rvprop=timestamp|user|size|comment&rvdir=newer&rvlimit=500
    api_call = ("https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=User_talk:{}&rvuser={}&rvdir=newer&rvprop=timestamp|user|size|comment&rvlimit=500&format=json").format(decode_wpid,decode_sid)#Kingsleyta
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    page_id = list(responsedata["query"]["pages"].keys())[0]
    page_title = responsedata['query']['pages'][page_id].get("title")
    try: 
        revision_data_lst=responsedata['query']['pages'][page_id]['revisions']
        for feature in revision_data_lst:
            csv_f.writerow([wpid_original, wpid, "user_talk", 3, page_title, feature.get('user'), feature['timestamp'], feature.get('size'), feature.get('comment')])
    except KeyError:
        pass
        #csv_f.writerow([wpid_original, wpid, "user_talk", 3, page_title, "no user talk", "no user talk","no user talk","no user talk"])
    n+=1    
    if n%100==0:
        print (n) 




















