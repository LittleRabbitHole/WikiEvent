#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 10:21:11 2018

@author: angli
"""
import pandas as pd
import numpy as np
import os
os.chdir("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")


###group the data per person
def user_aggre(data):
    aggre_data={}
    #group by wpid
    Grouped = data.groupby(['wpid'])
    
    n=0
    for pidgroup in Grouped:
        n+=1
        #if n==2: break
        wid = pidgroup[0]
        aggre_data[wid]={}
        
        #group on users
        userGrouped = pidgroup[1].groupby(['userid'])
        for usergroup in userGrouped:
            courseID = usergroup[0]
            aggre_data[wid][courseID]={}
            #WID
            WID = list(usergroup[1]["wpid"])[0]
            aggre_data[wid][courseID]['wpid'] = WID
            #userid
            userid = list(usergroup[1]["userid"])[0]
            aggre_data[wid][courseID]['userid'] = userid
            #total edit count
            edit_count = len(list(usergroup[1]['wpid']))
            aggre_data[wid][courseID]['article_edits'] = edit_count
            #ave article size diff
            all_df = usergroup[1][['ns','sizediff']]
            article_df = all_df.loc[all_df['ns'] == 0]
            article_sizediff = article_df['sizediff'].mean()
            aggre_data[wid][courseID]['article_sizediff'] = article_sizediff
            #number of articles
            title_lst = list(usergroup[1]['title'])
            unique_article_numbers = len(list(set(title_lst)))
            aggre_data[wid][courseID]['unique_articles'] = unique_article_numbers
            if n%100==0: print (n)
        
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
    #df_Contact.reset_index(level=1, inplace=True)
    #df_Contact.reset_index(inplace=True)
    
    #rename index column
    #df_Contact = df_Contact.rename(columns={'level_1': 'courseID'})
    #df_Contact = df_Contact.rename(columns={'index': 'wpid'})
    #df_Contact.columns.values

    #change columns order
    columns = ['wpid', 'userid', 
           'article_edits', 'article_sizediff', 'unique_articles']
    
    df_Contact = df_Contact[columns]
    
    return (df_Contact)



if __name__ == "__main__":

    contri_data = pd.read_csv("newcomer_article_contri_fourmonths.csv", encoding="latin-1") 
    contri_data.columns.values
    aggre_data = user_aggre(contri_data)
    
    aggre_data.to_csv("newcomers_4m_aggre_per_person.csv", index=False)
    
    contri_data["usertype"] = contri_data["usertype"].astype('category')
    contri_data["usertype"].describe()
