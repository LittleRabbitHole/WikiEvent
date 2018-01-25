#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 14:11:49 2017

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

os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
os.chdir("/Users/Ang/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")

data = pd.read_csv('newcomers_allevents.csv', encoding = "ISO-8859-1")
data.columns.values

data['register'] = data['register'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%y').strftime('%Y-%m-%d'))
data['onemonth'] = data['onemonth'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%y').strftime('%Y-%m-%d'))
data['fourmonth'] = data['fourmonth'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%y').strftime('%Y-%m-%d'))

data.to_csv("newcomers_allevents_months.txt", sep='\t', index=False)


fo = open("newcomers_allevents_months.txt")
lines=fo.readlines()
fo.close()


###newcomers contribution collections of 4 months data
f = open("newcomer_article_contri_fourmonths.csv", "w", encoding="UTF-8")
csv_f = csv.writer(f)
#write first row
csv_f.writerow(['user','userid', 'wpid', 'event', 'usertype',
                'timestamp', 'ns',  'title', 'pageid', 'revid','parentid', 
                'sizediff', 'minor'])

#write error
f_error=open("newcomer_edit_errors.csv", "w", encoding="UTF-8")

csv_error = csv.writer(f_error)


n=0
for line in lines[1::]:
    n+=1
    #print (n, line)
    line = line.strip()
    info_lst = line.split('\t')
    #userinfo
    userid = info_lst[1]
    print (n, userid)
    event = info_lst[-1]
    #usertype
    usertype = info_lst[-2]
    #get name from names list
    user = info_lst[0]
    #onemonth time window
    starttime = info_lst[2]+"T00:00:00Z"
    endtime = info_lst[4]+"T11:59:59Z"
    #get the contribution
    usercontri = GetUserContri(userid, starttime, endtime)
    if usercontri == "erroruser":
        csv_error.writerow([user])
    else:
        usercontribsdata = usercontri
        for feature in usercontribsdata:
            csv_f.writerow([user, feature['userid'], feature['user'], event, usertype,
                            feature['timestamp'],feature['ns'],feature['title'],
                            feature['pageid'], feature['revid'], feature['parentid'],
                            feature.get('sizediff','')])    
    #if n%10==0:

f.close()

##########################################
###collecting article quality
###collecting article quality
###collecting article quality
###collecting article quality

#This is to look at different vision as an article
#https://ores.wikimedia.org/v3/scores/enwiki?models=wp10&revids=712140761 

#This is to look at the diff (of one revision)
#https://ores.wikimedia.org/v3/scores/enwiki?models=goodfaith&revids=636074156
#https://ores.wikimedia.org/v3/scores/enwiki?models=reverted&revids=712140761
#https://ores.wikimedia.org/v3/scores/enwiki?models=goodfaith&revids=753383232|753383231|753383230|753383229|753383228

#read coursematch file
data = pd.read_csv("newcomer_article_contri_fourmonths.csv", encoding = "ISO-8859-1")
data.columns.values

userid_list = list(data['userid'])
usertyepe_list = list(data['usertype'])
event_list = list(data['event'])
revid_list = list(data['revid'])

revid_stack_list = [] #5156
n=0
revid_stack = ""
for revid in revid_list: 
    n+=1
    revid = str(revid)
    revid_prep = revid+"|"
    revid_stack += revid_prep
    if n%20 == 0:
        revid_stack_clean = revid_stack[:-1]
        revid_stack_list.append(revid_stack_clean)
        revid_stack = ""
        
f = open("revision_goodfaith_fourmonth.csv", "w", encoding="UTF-8")
csv_f = csv.writer(f)
#write first row

csv_f.writerow(['revid','prediction','prob-true','prob-false'])


#api_call = ("https://ores.wikimedia.org/v3/scores/enwiki?models=goodfaith&revids={}").format(revid_stack_clean)

# read username catch the Wiki data
n=0
for revid_stack_clean in revid_stack_list:
    #revid_lst = revid_stack_clean.split('|')
    #userid = row['userid']
    #usertype = row['usertype']
    #event = row['event']
    #api
    api_call = ("https://ores.wikimedia.org/v3/scores/enwiki?models=goodfaith&revids={}").format(revid_stack_clean)#636074156
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    #responsedata['enwiki']['scores'].keys()
    page_id_lst = list(responsedata['enwiki']['scores'].keys())
    for page_id in page_id_lst:
        score_data = responsedata['enwiki']['scores'][page_id]['goodfaith'].get('score','error')
        if score_data != "error": 
            #error_msg = responsedata['enwiki']['scores'][page_id]['goodfaith']['error'][message]
            #csv_f.writerow([ page_id, prediction, prob_true, prob_false])
            prediction = score_data['prediction']
            prob_true = score_data['probability']['true']
            prob_false = score_data['probability']['false']  
            #print ([ page_id, prediction, prob_true, prob_false])
            csv_f.writerow([ page_id, prediction, prob_true, prob_false])
    n+=1
    if n%10==0:
        print (n) 

f.close()


###merge quality data with contribution data
contri_data = pd.read_csv("newcomer_article_contri_fourmonths.csv", encoding = "ISO-8859-1")
contri_data = contri_data.drop_duplicates()
contri_data.columns.values
#len(set(list(contri_data['revid'])))

qual_data = pd.read_csv("revision_goodfaith_fourmonth.csv", encoding = "ISO-8859-1")
qual_data.columns.values
qual_data = qual_data.drop_duplicates()
len(set(list(qual_data['revid'])))

contri_qual = pd.merge(contri_data, qual_data, how = 'left', on = "revid")
contri_qual = contri_qual.dropna()
contri_qual.columns.values
contri_qual = contri_qual[['user', 'userid', 'wpid', 'event', 'timestamp', 'ns',
       'title', 'pageid', 'revid', 'parentid', 'sizediff', 'prediction',
       'prob-true', 'prob-false']]

user_type =pd.read_csv("newcomers_allevents.csv")
user_type.columns.values
user_type = user_type[['userid','user_type']]

newcomers_contri_qual = pd.merge(contri_qual, user_type, how = 'left', on = "userid")
newcomers_contri_qual = newcomers_contri_qual.dropna()

newcomers_contri_qual.to_csv("newcomers_contri_quality.csv", index=False)

##aggregation based on per-person
aggre_data={}
#group by wpid
Grouped = newcomers_contri_qual.groupby(['wpid'])

n=0
for pidgroup in Grouped:
    n+=1
    #if n==2: break
    wid = pidgroup[0]
    aggre_data[wid]={}
    
    piddata = pidgroup[1]#.groupby(['after_first_event_edit'])    
    studentID = list(piddata["user"])[0]
    aggre_data[wid]['user'] = studentID    
    #usertype
    user_type = list(piddata['user_type'])[0]
    aggre_data[wid]['user_type'] = user_type        
    #userid
    userid = list(piddata["userid"])[0]
    aggre_data[wid]['userid'] = userid
    #event
    user_event = list(piddata["event"])[0]
    aggre_data[wid]['event'] = user_event
    #total edit count
    edit_count = len(list(piddata['wpid']))
    aggre_data[wid]['edit_count'] = edit_count
    #ave true
    ave_true = sum(list(piddata['prob-true']))/(len(list(piddata['prob-true'])) + 0.0001)
    aggre_data[wid]['ave_true'] = ave_true
    #ave false
    ave_false = sum(list(piddata['prob-false']))/(len(list(piddata['prob-false'])) + 0.0001)
    aggre_data[wid]['ave_false'] = ave_false
    # prodiction count
    predict_list = list(piddata['prediction'])
    predict_true_count = predict_list.count(True) #edits in article
    predict_false_count = predict_list.count(False) #edits in article
    aggre_data[wid]['predict_true_count'] = predict_true_count
    aggre_data[wid]['predict_false_count'] = predict_false_count

    if n%100==0: print (n)

bf_data = pd.DataFrame.from_dict(aggre_data, orient='index')
#change columns order

columns = ["user", 'userid','user_type','event','edit_count', 'ave_true', 'ave_false', 'predict_true_count', 'predict_false_count']

bf_data = bf_data[columns]

#bf_data.to_csv("editor_aggre_firstday.csv", index=False)
bf_data.to_csv("editor_goodfalse_aggre.csv", index=False)

##################c######################################################
##################c######################################################
###collecting revert data########
###collecting revert data########
###collecting revert data########
###collecting revert data########
##check reverting

import mwapi, mwreverts.api

# Gather a page's revisions from the API
session = mwapi.Session("https://en.wikipedia.org", user_agent="mwreverts basic usage script")


#An edit can reverting other edits, it can be reverted, or it can be reverted_to by another edit.
def print_revert_status(rev_id, reverting, reverted, reverted_to):
    """Prints a nice, pretty version of a revert status."""
    print(str(rev_id) + ":")
    if reverting is not None:
        print(" - reverting {0} other edits".format(len(reverting.reverteds)))
    if reverted is not None:
        print(" - reverted in {revid} by {user}".format(**reverted.reverting))
    if reverted_to is not None:
        print(" - reverted_to in {revid} by {user}".format(**reverted_to.reverting))

reverting, reverted, reverted_to = mwreverts.api.check(session, 616034852, rvprop={'user'})
print_revert_status(616034852, reverting, reverted, reverted_to)
reverted.reverting


#whether reverted, 1=yes
def revert_status(rev_id, reverted):
    reverting, reverted, reverted_to = mwreverts.api.check(session, rev_id, rvprop={'user'})
    if reverted is not None: 
        return 1
    else: 
        return 0


qual_data = pd.read_csv("revision_goodfaith_fourmonth.csv", encoding = "ISO-8859-1")
qual_data.columns.values
qual_data = qual_data.drop_duplicates()
len(set(list(qual_data['revid'])))

revert_dict = {}
n=0
for index, row in qual_data.iterrows():
    n+=1
    revid = row['revid']
    try:
        revert_mark = revert_status(revid, reverted)
        revert_dict[revid]=revert_mark
    except mwapi.errors.APIError:
        revert_dict[revid]= -1 #error
    if n%50==0: 
        print (n)
    
revert_data = pd.DataFrame.from_dict(revert_dict, orient='index')
revert_data.reset_index(inplace=True)
revert_data = revert_data.rename(columns={'index': 'revid'})
revert_data = revert_data.rename(columns={0: 'reverted'})
revert_data.columns.values
revert_data.to_csv("revert_data.csv", index=False)

###merge revert data with contribution data
contri_data = pd.read_csv("newcomer_article_contri_fourmonths.csv", encoding = "ISO-8859-1")
contri_data = contri_data.drop_duplicates()
contri_data.columns.values
#len(set(list(contri_data['revid'])))

revert_data = pd.read_csv("revert_data.csv", encoding = "ISO-8859-1")
revert_data.columns.values
revert_data = revert_data.drop_duplicates()
len(set(list(revert_data['revid'])))

contri_revert = pd.merge(contri_data, revert_data, how = 'left', on = "revid")
contri_revert = contri_revert.dropna()
contri_revert.columns.values
contri_revert = contri_revert[['user', 'userid', 'wpid', 'event', 'timestamp', 'ns',
       'title', 'pageid', 'revid', 'parentid', 'sizediff', 'reverted']]

user_type =pd.read_csv("newcomers_allevents.csv")
user_type.columns.values
user_type = user_type[['userid','user_type']]

newcomers_contri_revert = pd.merge(contri_revert, user_type, how = 'left', on = "userid")
newcomers_contri_revert = newcomers_contri_revert.dropna()
newcomers_contri_revert.columns.values

newcomers_contri_revert.to_csv("newcomers_contri_revert.csv", index=False)

##aggregation based on per-person
aggre_data={}
#group by wpid
Grouped = newcomers_contri_revert.groupby(['wpid'])

n=0
for pidgroup in Grouped:
    n+=1
    #if n==2: break
    wid = pidgroup[0]
    aggre_data[wid]={}
    
    piddata = pidgroup[1]#.groupby(['after_first_event_edit'])    
    user = list(piddata["user"])[0]
    aggre_data[wid]['user'] = user    
    #usertype
    user_type = list(piddata['user_type'])[0]
    aggre_data[wid]['user_type'] = user_type        
    #userid
    userid = list(piddata["userid"])[0]
    aggre_data[wid]['userid'] = userid
    #event
    user_event = list(piddata["event"])[0]
    aggre_data[wid]['event'] = user_event
    #total edit count
    edit_count = len(list(piddata['wpid']))
    aggre_data[wid]['edit_count'] = edit_count
    #revert
    revert_list = list(piddata['reverted'])
    no_revert_count = revert_list.count(0) 
    revert_count = revert_list.count(1)
    aggre_data[wid]['revert_count'] = revert_count
    revert_ratio = revert_count/(revert_count + no_revert_count)
    aggre_data[wid]['revert_ratio'] = revert_ratio
    if n%100==0: print (n)

revert_data = pd.DataFrame.from_dict(aggre_data, orient='index')
#change columns order
revert_data.columns.values

columns = ["user", 'userid','user_type','event','edit_count', 'revert_count', 'revert_ratio']

revert_data = revert_data[columns]

revert_data.to_csv("user_revert_aggre.csv", index=False)


##################c######################################################
#collection detailed reverted information
##################c######################################################
##################c######################################################
##################c######################################################
def revertInfo(rev_id):
    reverting, reverted, reverted_to = mwreverts.api.check(session, rev_id, rvprop={'user'})
    if reverted is not None:
        reverted_info = reverted.reverting
        revertedby_revid = reverted_info['revid']
        revertedby_parentid = reverted_info['parentid']
        reverted_pageid = reverted_info['page']['pageid']
        reverted_title = reverted_info['page']['title']
        revert_info_lst = [revertedby_parentid, reverted_pageid, reverted_title,revertedby_revid]
        return ([revert_info_lst])

    

a=revertInfo(662176088)

reverting, reverted, reverted_to = mwreverts.api.check(session, 692798059, rvprop={'user'})
#https://en.wikipedia.org/w/index.php?title=Black_Lives_Matter&diff=next&oldid=662176088
#revert_status(477059772, reverting, reverted, reverted_to)
reverted_info = reverted.reverting
reverted_info['revid']
reverted_info['parentid']
reverted_info['page']['pageid']
reverted_info['page']['title']

#api: https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=ids|userid|user|comment|comment&revids=662239560
#https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=ids|userid|user|comment|comment&revids=654547111


revert_data = pd.read_csv("revert_data.csv", encoding = "ISO-8859-1")
revert_data.columns.values
revert_data = revert_data.drop_duplicates()
revert_revids = revert_data.loc[revert_data['reverted']==1]
reverted_revid_lst = list(revert_revids['revid'])
len(set(reverted_revid_lst))


revert_info_dict = {}
n=0
for revid in reverted_revid_lst:
    n+=1
    try:
        revert_info = revertInfo(revid)
        revert_info_dict[revid]=revert_info
    except mwapi.errors.APIError:
        revert_dict[revid]= [] #error
    if n%50==0: 
        print (n)

pickle.dump( revert_info_dict, open( "revert_info_dict.p", "wb" ) )



keys = []
#revertedby_parentid, reverted_pageid, reverted_title,revertedby_revid
list1 = []
list2 = []
list3 = []
list4 = []

for k in revert_info_dict:
    keys.append(k)
    list1.append(revert_info_dict[k][0][0])
    list2.append(revert_info_dict[k][0][1])
    list3.append(revert_info_dict[k][0][2])
    list4.append(revert_info_dict[k][0][3])

revert_info = pd.DataFrame({ 'revid': keys, 'revertedby_parent_revid': list1, 'reverted_pageid': list2, 'reverted_title': list3, 'revertedby_revid': list4}, index=keys)
revert_info.columns.values
columns = ['revid', 'revertedby_parent_revid', 'reverted_pageid', 'reverted_title', 'revertedby_revid']
revert_info = revert_info[columns]
revert_info.to_csv("revert_info.csv", index=True)


##collecting comment for each revert_revid
revertedby_revid_lst =  list4


f = open("revvert_comments_fourmonth.csv", "w", encoding="UTF-8")
csv_f = csv.writer(f)
#write first row

csv_f.writerow(['revertedby_revid','reverted_byrevid','userid','user', 'parentid','comment'])


#api_call = ("https://ores.wikimedia.org/v3/scores/enwiki?models=goodfaith&revids={}").format(revid_stack_clean)

# read username catch the Wiki data
n=0
for revertedby_revid in revertedby_revid_lst:
    api_call = ("https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=ids|userid|user|comment&revids={}&format=json").format(revertedby_revid)#636074156
    response=urllib.request.urlopen(api_call)
    str_response=response.read().decode('utf-8')
    responsedata = json.loads(str_response)
    page_id = list(responsedata['query']['pages'].keys())[0]
    revision_data = responsedata['query']['pages'][page_id]['revisions'][0]
    comment = revision_data['comment'].replace(",","")
    reverted_byrevid = revision_data['revid']
    user = revision_data['user']
    userid = revision_data['userid']
    parentid = revision_data['parentid']
    #print ([ page_id, prediction, prob_true, prob_false])
    csv_f.writerow([ revertedby_revid, reverted_byrevid, userid, user, parentid, comment])
    n+=1
    if n%50==0:
        print (n) 

f.close()

##merge comment back to reverted edit by event newcomer
revert_data = pd.read_csv("revert_info.csv", encoding = "ISO-8859-1")
revert_data.columns.values

reverted_comment = pd.read_csv("revvert_comments_fourmonth.csv", encoding = "ISO-8859-1")
reverted_comment.columns.values

revert_data_comment = pd.merge(revert_data, reverted_comment, on="revertedby_revid")
revert_data_comment.columns.values


#merge usertype and event
data = pd.read_csv("newcomers_contri_quality.csv", encoding = "ISO-8859-1")
data.columns.values


revert_comment_usertype = pd.merge(revert_data_comment, data, on="revid")
revert_comment_usertype.to_csv("revert_comment_usertype.txt", sep="\t",index=False)


##get the wiki-link formmat
import numpy as np
import re
revert_comment = pd.read_csv("revert_comment_usertype.csv", encoding = "ISO-8859-1")
revert_comment = revert_comment[['revid', 'title','userid', 'wpid', 'event','user_type','prob-true', 'reverted_byrevid', 'reverted_comment']] 
revert_comment.columns.values
comment_list = list(revert_comment['reverted_comment'])
wikitemp_types = pd.read_csv("revert_wikitemps_todic.csv")
wikitemp_types = wikitemp_types[['WikiTemp','category']]
wikitemp_types = wikitemp_types.set_index('WikiTemp')
wikitemp_dict = wikitemp_types.to_dict('index')

wikitemp = re.compile('\[\[WP:[^\[\]]*\]\]')
#revert_wikitemp = []


revert_citation_source_link_verification = []
revert_ccopyright = []
revert_disambiguation = []
revert_goodFaithEdit = []
revert_incivility = []
#revert_link = []
revert_neutral = []
revert_notable = []
revert_orginalResearch = []
#revert_source = []
revert_rules = []
#revert_style = []
revert_vandal = []
#revert_verification = []
revert_wordingwritingstyle = []
#revert_writing = []

for index, row in revert_comment.iterrows():
    comment = row['reverted_comment']
    if pd.isnull(comment):
        revert_citation_source_link_verification.append(0)
        revert_ccopyright.append(0)
        revert_disambiguation.append(0)
        revert_goodFaithEdit.append(0)
        revert_incivility.append(0)
        #revert_link.append(0)
        revert_neutral.append(0)
        revert_notable.append(0)
        revert_orginalResearch.append(0)
        #revert_source.append(0)
        revert_rules.append(0)
        #revert_style.append(0)
        revert_vandal.append(0)
        #revert_verification.append(0)
        revert_wordingwritingstyle.append(0)
        #revert_writing.append(0)
    else:
        matche_pov = re.findall('pov', comment, re.IGNORECASE)
        matche_pov2 = re.findall('neutra', comment, re.IGNORECASE)
        matche_source = re.findall('source', comment, re.IGNORECASE)
        matche_source2 = re.findall("reliable", comment, re.IGNORECASE)
        matche_vanda = re.findall('vanda', comment, re.IGNORECASE)
        matche_link = re.findall('link', comment, re.IGNORECASE)
        matche_citation = re.findall('citation', comment, re.IGNORECASE)
        matche_wikitemp = wikitemp.findall(comment)
        #revert_pov.append(len(matche_pov))
        #revert_source.append(len(matche_source))
        #revert_vanda.append(len(matche_vanda))
        #revert_link.append(len(matche_link))
        #revert_wikitemp.append(matche_wikitemp)
        wikitemp_types = []
        for item in matche_wikitemp:
            try: 
                wikitemp_types.append(wikitemp_dict.get(item).get('category'))
            except AttributeError:
                pass
        citation = wikitemp_types.count("citation")
        link = len(matche_link) + wikitemp_types.count("link")
        sources = len(matche_source) +len(matche_source2)+ wikitemp_types.count("sources")
        verification = wikitemp_types.count("verification")
        #revert_verification.append(int(verification>0))
        citation_source_link_verification = citation+sources+link+verification
        revert_citation_source_link_verification.append(int(citation_source_link_verification>0))
        ccopyright = wikitemp_types.count("copyright")
        revert_ccopyright.append(int(ccopyright>0)) 
        disambiguation = wikitemp_types.count("disambiguation")
        revert_disambiguation.append(int(disambiguation>0))        
        goodfaithEdit = wikitemp_types.count("good faith edit")
        revert_goodFaithEdit.append(int(goodfaithEdit>0))
        incivility = wikitemp_types.count("incivility")
        revert_incivility.append(int(incivility>0))
        #link = len(matche_link) + wikitemp_types.count("link")
        #revert_link.append(int(link>0))
        neutral = len(matche_pov) + len(matche_pov2) + wikitemp_types.count("neutral")
        revert_neutral.append(int(neutral>0))
        notable = wikitemp_types.count("notable")
        revert_notable.append(int(notable>0))
        orginalResearch = wikitemp_types.count("orginal research")
        revert_orginalResearch.append(int(orginalResearch>0))
        #sources = len(matche_source) +len(matche_source2)+ wikitemp_types.count("sources")
        #revert_source.append(int(sources>0))
        rules = wikitemp_types.count("Rules")
        revert_rules.append(int(rules>0))
        vandal = len(matche_vanda) + wikitemp_types.count("vandal")
        revert_vandal.append(int(vandal>0))
        wording = wikitemp_types.count("wording issue")
        #revert_wording.append(int(wording>0))
        writing = wikitemp_types.count("writing issue")
        #revert_writing.append(int(writing>0))
        style = wikitemp_types.count("style")
        wordingwritingstyle = wording + writing + style
        revert_wordingwritingstyle.append(int(wordingwritingstyle>0))



revert_comment['citation_source_link_verification'] = revert_citation_source_link_verification
revert_comment['copyright'] = revert_ccopyright
revert_comment['disambiguation'] =revert_disambiguation
revert_comment['revert_goodFaithEdit'] = revert_goodFaithEdit
revert_comment['incivility'] = revert_incivility
#revert_comment['revert_link'] = revert_link
revert_comment['neutral'] = revert_neutral
revert_comment['notable'] = revert_notable
revert_comment['orginalResearch'] = revert_orginalResearch
#revert_comment['revert_source'] = revert_source
revert_comment['rules'] = revert_rules
revert_comment['wordingwritingstyle'] = revert_wordingwritingstyle
revert_comment['vandal'] = revert_vandal
#revert_comment['revert_verification'] = revert_verification
#revert_comment['revert_wording'] = revert_wording
#revert_comment['revert_writing'] = revert_writing

revert_comment = revert_comment.drop_duplicates()
revert_comment.to_csv("revert_comment_reverttype.csv", index=False)


revert_comment = pd.read_csv("revert_comment_reverttype.csv", encoding = "ISO-8859-1")
revert_comment.columns.values
#revert_comment = revert_comment.dropna()
#revert_comment = revert_comment.loc[revert_comment['check']==1]
revert_comment = revert_comment[['userid', 'wpid', 'event', 'user_type',
                                 'citation_source_link_verification', 
                                 'copyright', 'disambiguation',
                                 'revert_goodFaithEdit', 'incivility', 
                                 'neutral', 'notable', 
                                 'orginalResearch',
                                 'rules', 'vandal',
                                  'wordingwritingstyle']]
##aggregation based on per-person
aggre_data={}
#group by wpid
Grouped = revert_comment.groupby(['wpid'])

n=0
for pidgroup in Grouped:
    n+=1
    #if n==2: break
    wid = pidgroup[0]
    aggre_data[wid]={}
    
    piddata = pidgroup[1]#.groupby(['after_first_event_edit'])    
    #usertype
    user_type = list(piddata['user_type'])[0]
    aggre_data[wid]['user_type'] = user_type        
    #userid
    userid = list(piddata["userid"])[0]
    aggre_data[wid]['userid'] = userid
    #event
    user_event = list(piddata["event"])[0]
    aggre_data[wid]['event'] = user_event
    #total revert count
    revert_count = len(list(piddata['wpid']))
    aggre_data[wid]['revert_count'] = revert_count
    #revert citation
    revert_citation_source_link_verification = piddata['citation_source_link_verification'].sum()
    aggre_data[wid]['citation_source_link_verification'] = revert_citation_source_link_verification/(revert_count+0.001)
    revert_ccopyright = piddata['copyright'].sum()
    aggre_data[wid]['copyright'] = revert_ccopyright/(revert_count+0.001)
    disambiguation = piddata['disambiguation'].sum()
    aggre_data[wid]['disambiguation'] = disambiguation/(revert_count+0.001)
    revert_goodFaithEdit = piddata['revert_goodFaithEdit'].sum()
    aggre_data[wid]['revert_goodFaithEdit'] = revert_goodFaithEdit/(revert_count+0.001)
    revert_incivility = piddata['incivility'].sum()
    aggre_data[wid]['incivility'] = revert_incivility/(revert_count+0.001)
    #revert_link = piddata['revert_link'].sum()
    #aggre_data[wid]['revert_link'] = revert_link/(revert_count+0.001)
    revert_neutral = piddata['neutral'].sum()
    aggre_data[wid]['neutral'] = revert_neutral/(revert_count+0.001)
    revert_notable = piddata['notable'].sum()
    aggre_data[wid]['notable'] = revert_notable/(revert_count+0.001)
    revert_orginalResearch = piddata['orginalResearch'].sum()
    aggre_data[wid]['orginalResearch'] = revert_orginalResearch/(revert_count+0.001)
    #revert_source = piddata['revert_source'].sum()
    #aggre_data[wid]['revert_source'] = revert_source/(revert_count+0.001)
    wordingwritingstyle = piddata['wordingwritingstyle'].sum()
    aggre_data[wid]['wordingwritingstyle'] = wordingwritingstyle/(revert_count+0.001)
    revert_vandal = piddata['vandal'].sum()
    aggre_data[wid]['vandal'] = revert_vandal/(revert_count+0.001)
    #revert_verification = piddata['revert_verification'].sum()
    #aggre_data[wid]['revert_verification'] = revert_verification/(revert_count+0.001)
    #revert_wording = piddata['revert_wording'].sum()
    #aggre_data[wid]['revert_wording'] = revert_wording/(revert_count+0.001)
    #revert_writing = piddata['revert_writing'].sum()
    #aggre_data[wid]['revert_writing'] = revert_writing/(revert_count+0.001)
    if n%100==0: print (n)

revert_data = pd.DataFrame.from_dict(aggre_data, orient='index')
#change columns order
revert_data.columns.values

columns = ['userid', 'event', 'user_type','revert_count',
           'citation_source_link_verification', 'copyright', 'disambiguation',
           'revert_goodFaithEdit', 'incivility', 
           'neutral', 'notable', 'orginalResearch',
           'wordingwritingstyle', 'vandal']

revert_data = revert_data[columns]

revert_data.to_csv("revert_reason_user_aggre_withcheck.csv", index=True)





f = open("revert_wikitemps.csv", "w", encoding="UTF-8")
csv_f = csv.writer(f)
for wikitemp in wiki_temp:
    csv_f.writerow([wikitemp])    
f.close()



###why use four month

data = pd.read_csv('newcomers_contri_quality.csv', encoding = "ISO-8859-1")
data.columns.values
data["date"]=data['timestamp'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').strftime('%m/%d/%Y'))
#data['date'] = pd.to_datetime(data['date'])
aggr_cases = data.groupby(['date']).mean()
aggr_cases.to_csv("newcomers_daily_quality.csv", index=True)
