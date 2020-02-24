require(lme4)
library(survminer)
library(survival)
library(KMsurv)
library(coxme)

setwd("/Users/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
setwd("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")

#####collective measures on revert#####
contri_data = read.csv("newcomers_contri_revert_quality_1month_mark.csv")
extreme_user = as.character(contri_data$user[contri_data$sizediff==min(contri_data$sizediff)])
contri_data = contri_data[which(contri_data$user != extreme_user),]
min(contri_data$sizediff)

contri_data$eventgroup = 0
contri_data$eventgroup[which(contri_data$first_edit_type3==1)] = 1 #for event


colnames(contri_data)

event = contri_data[which(contri_data$eventgroup==1 & contri_data$time_marker_1m == 1),]
event_revert = event[which(event$reverted == 1),]
sum(event$sizediff)
sum(event_revert$sizediff)/sum(event$sizediff)

control = contri_data[which(contri_data$eventgroup ==0 & contri_data$time_marker_1m == 1),]
control_revert = control[which(control$reverted == 1),]
sum(control$sizediff)
sum(control_revert$sizediff)/sum(control$sizediff)


wiki = contri_data[which(contri_data$first_edit_type3==2& contri_data$time_marker_1m == 1),]
wiki_revert = wiki[which(wiki$reverted == 1),]
sum(wiki$sizediff)
sum(wiki_revert$sizediff)/sum(wiki$sizediff)

social = contri_data[which(contri_data$first_edit_type3==3& contri_data$time_marker_1m == 1),]
social_revert = social[which(social$reverted == 1),]
sum(social$sizediff)
min(social$sizediff, na.rm = TRUE)

sum(c(social_revert$sizediff)[! c(social_revert$sizediff) %in% c(-91925)])/sum(social$sizediff)

####merging data###
wikidata =  read.csv("firstweek_factors_outcomes.csv")
colnames(wikidata)
wikidata$event <- tolower(wikidata$event)

network = read.csv("talk_network_measure_1w_data.csv")
colnames(network)

quality_revert = read.csv("qual_factors_outcomes.csv")
colnames(quality_revert)
quality_revert$event <- tolower(quality_revert$event)

retention = read.csv("Newcomers_groups.csv")
colnames(retention)
retention$event <- tolower(retention$event)

####merge
wiki_production = merge(wikidata, network, by = c("wpid", "first_edit_type3"))
colnames(wiki_production)

wikidata_rp = merge(retention, wiki_production, by = c("wpid", "userid", "first_edit_type3", "event"))
colnames(wikidata_rp)

wikidata_rpq = merge(wikidata_rp, quality_revert, by = c("wpid", "userid", "first_edit_type3", "event"))

colnames(wikidata_rpq)

write.csv(wikidata_rpq, "wikievent_full_data.csv", row.names = FALSE)

#####start analysis######

data = read.csv("wikievent_full_data.csv")
colnames(data)

######retention, survival analysis########
table(data$event, data$first_edit_type3)
#newcomers_list = data[c("wpid","userid")]

data$SurvObj <- with(data, Surv(time, death == 1))
colnames(data)

data$eventgroup = -1
data$eventgroup[which(data$first_edit_type3==1)] = 2 #for event
data$Social = 0
#data$Social[which(data$first_edit_type3==2)] = -1 #wikipedian
data$Social[which(data$first_edit_type3==3)] = 1 #social

mean(data$after_article_sizediff[which(data$eventgroup == 2)], na.rm = TRUE)
mean(data$after_article_sizediff[which(data$eventgroup == -1)], na.rm = TRUE)

summary(as.factor(data$eventgroup))
summary(as.factor(data$Social))

data$norm_weighted_Indegree = (data$weighted_Indegree-min(data$weighted_Indegree,na.rm =TRUE))/(max(data$weighted_Indegree,na.rm =TRUE)-min(data$weighted_Indegree,na.rm =TRUE))
data$norm_weighted_Outdegree = (data$weighted_Outdegree-min(data$weighted_Outdegree,na.rm =TRUE))/(max(data$weighted_Outdegree,na.rm =TRUE)-min(data$weighted_Outdegree,na.rm =TRUE))
data$event_indegree = data$eventgroup*data$norm_weighted_Indegree
data$event_outdegree = data$eventgroup*data$norm_weighted_Outdegree
data$event_eigen = data$eventgroup*data$eigen

model <- coxph(SurvObj ~ as.factor(eventgroup) 
               + norm_weighted_Indegree 
               + norm_weighted_Outdegree
               + scale(eigen)
               + log(before_unique_articles+0.01)
               + log(before_talk_count + 0.01) 
               + log(before_user_count + 0.01)
               + event_indegree
               +event_outdegree
               +event_eigen
               + log(before_revert_count + 0.01)
               + cluster(event), 
               data = data)
summary(model) 


model <- coxme(SurvObj ~ as.factor(eventgroup) 
               + norm_weighted_Indegree 
               + norm_weighted_Outdegree
               + scale(eigen)
               + log(before_unique_articles+0.01)
               + log(before_talk_count + 0.01) 
               + log(before_user_count + 0.01)
               + log(before_revert_count + 0.01)
               + (1|event), 
               data = data)
summary(model)

#production
data$norm_talk_count = log(data$before_talk_count + 0.01)
data$norm_user_count  = log(data$before_user_count + 0.01)
data$norm_usertalk_count  = log(data$before_usertalk_count + 0.01)
data$norm_unique_articles  = log(data$before_unique_articles + 0.01)

model = lmer(log(after_article_count+0.01) ~ as.factor(eventgroup) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             + eigen
             + norm_talk_count 
             + norm_user_count
             + norm_unique_articles
             + event_indegree
             +event_outdegree
             +event_eigen
             + log(before_revert_count + 0.01)
             + (1|event)
             , data = data)
summary(model)
dt(1.517, df=length(data) - 1)

#article size
model = lmer(scale(after_article_sizediff) ~ as.factor(eventgroup)
             + norm_weighted_Indegree + norm_weighted_Outdegree
             + eigen
             + norm_talk_count 
             + norm_user_count
             + norm_unique_articles
             + event_indegree
             +event_outdegree
             +event_eigen
             + (1|event)
             , data = data)

summary(model)
dt(2.01, df=length(data) - 1)

#quality
model = lmer(scale(after_ave_true) ~ as.factor(eventgroup) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             + eigen
             + norm_talk_count 
             + norm_user_count
             + norm_unique_articles
             + log(before_article_count+0.01)
             + log(before_revert_count + 0.01)
             + (1|event)
             , data = data)
summary(model)


model = lmer(scale(after_revert_ratio) ~ as.factor(eventgroup) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             + eigen
             + norm_talk_count 
             + norm_user_count
             + norm_unique_articles
             + log(before_article_count+0.01)
             + log(before_revert_count + 0.01)
             + (1|event)
             , data = data)
summary(model)


data$reverted = data$after_revert_count >=1

model = lmer(log(after_revert_count + 0.01) ~ as.factor(eventgroup) 
             + as.factor(Social) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             + eigen
             + norm_talk_count 
             + norm_user_count
             + norm_unique_articles
             + log(after_article_count+0.01)
             + log(before_revert_count + 0.01)
             + (1|event)
             , data = data)
summary(model)

##social network
model = lmer(eigen ~ as.factor(eventgroup) 
             + as.factor(Social) 
             #+ norm_weighted_Indegree + norm_weighted_Outdegree
             #+ Indegree_norm + Outdegree_norm
             #+ norm_betweenness #+ closenessnorm
             #+ eigen
             #+ norm_talk_count 
             #+ norm_user_count
             #+ norm_usertalk_count
             #+ norm_unique_articles
             #+ log(before_article_count+0.01)
             #+ before_ave_true
             #+ before_revert_ratio
             #+ log(before_revert_count + 0.01)
             + (1|event)
             #,family = poisson
             , data = data)
summary(model)

