#this code is analysis of user retenion, and edit quality
require(lme4)
library(survminer)
library(survival)
library(KMsurv)
library(coxme)

setwd("/Users/Ang/Documents/GitHub/WikiEvent/data")
####merging data###
wikidata =  read.csv("newcomers_4m_aggre_per_person.csv")
outcome_revert = read.csv("editor_revert.csv")
network = read.csv("network.csv")
network = unique(network)

n_occur <- data.frame(table(network$wpid))
network[network$wpid %in% n_occur$Var1[n_occur$Freq > 1],]
network <- network[-c(306), ]

wikidata1 = merge(wikidata, network, by = "wpid", all.x = TRUE)
wikidata2 = merge(wikidata1, outcome_revert, by = c("wpid", "userid"), all.x = TRUE)

write.csv(wikidata2, "wikievent_data.csv", row.names = FALSE)
####done####
data =  read.csv("wikievent_data.csv")
colnames(data)

#retention
table(data$event, data$first_edit_type3)
newcomers_list = data[c("wpid","userid")]

data$SurvObj <- with(data, Surv(time, death == 1))
colnames(data)

data$eventgroup = -1
data$eventgroup[which(data$first_edit_type3==1)] = 2 #for event
data$Social = 0
#data$Social[which(data$first_edit_type3==2)] = -1 #wikipedian
data$Social[which(data$first_edit_type3==3)] = 1 #social

summary(as.factor(data$eventgroup))
summary(as.factor(data$Social))

data$norm_weighted_Indegree = (data$weighted_Indegree-min(data$weighted_Indegree,na.rm =TRUE))/(max(data$weighted_Indegree,na.rm =TRUE)-min(data$weighted_Indegree,na.rm =TRUE))
data$norm_weighted_Outdegree = (data$weighted_Outdegree-min(data$weighted_Outdegree,na.rm =TRUE))/(max(data$weighted_Outdegree,na.rm =TRUE)-min(data$weighted_Outdegree,na.rm =TRUE))
data$norm_betweenness = (data$betweenness-min(data$betweenness,na.rm =TRUE))/(max(data$betweenness,na.rm =TRUE)-min(data$betweenness,na.rm =TRUE))


model <- coxph(SurvObj ~ as.factor(eventgroup) + as.factor(Social)
               + norm_weighted_Indegree + norm_weighted_Outdegree
               #+ Indegree_norm + Outdegree_norm
               + norm_betweenness #+ closenessnorm
               + eigen
               + cluster(event), 
               data = data)
summary(model) 

#edit quality
model = lmer(ave_true ~ as.factor(eventgroup) + as.factor(Social) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             #+ Indegree_norm + Outdegree_norm
             + norm_betweenness #+ closenessnorm
             + eigen
             #+ article_edits 
             #+ unique_articles 
             + (1|event)
             , data = data)
summary(model)


##production
model = lmer(log(article_edits+0.01) ~ as.factor(eventgroup) + as.factor(Social) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             #+ Indegree_norm + Outdegree_norm
             + norm_betweenness #+ closenessnorm
             + eigen
             #+ article_edits 
             #+ unique_articles 
             + (1|event)
             , data = data)
summary(model)

#article size
model = lmer(scale(article_sizediff) ~ as.factor(eventgroup) + as.factor(Social) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             #+ Indegree_norm + Outdegree_norm
             + norm_betweenness #+ closenessnorm
             + eigen
             #+ article_edits 
             #+ unique_articles 
             + (1|event)
             , data = data)
summary(model)

#revert
model = lmer(scale(revert_ratio) ~ as.factor(eventgroup) + as.factor(Social) 
             + norm_weighted_Indegree + norm_weighted_Outdegree
             #+ Indegree_norm + Outdegree_norm
             + norm_betweenness #+ closenessnorm
             + eigen
             + log(article_edits+0.01) 
             + log(unique_articles +0.01)
             + (1|event)
             , data = data)
summary(model)


###revert
data = read.csv("newcomers_contri_revert.csv")

data = data[which(data$reverted > -1),]
data = merge(data, newcomers_list, by = c("userid", "user"))
data  = data[!duplicated(data), ]

#revert summary
colnames(data)
length(unique(data$userid)) 
summary(as.factor(data$reverted))
#0     1 
#65538  4902 
length(unique(data$userid[which(data$reverted == 1)])) #344 newcomers get reverted

summary(as.factor(data$user_type))
data$groupA = -2
data$groupA[which(data$user_type==1)] = 1 #for group A
data$groupC = 0
data$groupC[which(data$user_type==2)] = -1 #group B = -1; Group c = 1; group a = 0
data$groupC[which(data$user_type==3)] = 1

#revert model
summary(as.factor(data$reverted))
model = glmer(reverted ~  as.factor(groupA) + as.factor(groupC) +  (1|event) + (1|userid), data = data, family = binomial)
summary(model)

data$fifa = -2
data$fifa[which(data$event=='fifa')] = 1 #for fifa
model = glmer(reverted ~ as.factor(fifa) + (1|userid), data = data[which(data$user_type==1),], family = binomial)
summary(model)


#revert reasons
revert_data = read.csv("revert_comment_reverttype.csv")
revert_data = merge(revert_data, newcomers_list, by ="userid")
colnames(revert_data)
revert_data = unique(revert_data)
length(unique(revert_data$userid))
#comments = revert_data$reverted_comment
#write.csv(revert_data, "revert_data_comment2.csv")
revert_data$check2 <- revert_data$vandal + revert_data$citation_source_link_verification + 
  revert_data$orginalResearch + revert_data$neutral + 
  revert_data$copyright + revert_data$disambiguation + 
  revert_data$incivility + revert_data$notable + revert_data$wordingwritingstyle #+ revert_data$revert_goodFaithEdit

sum(revert_data$check2)
revert_data=revert_data[which(revert_data$check2>0),]

table(revert_data$user_type, revert_data$event)
colnames(revert_data)
summary(as.factor(revert_data$user_type))
summary(as.factor(revert_data$event))
sum(revert_data$vandal)
table(revert_data$user_type, revert_data$vandal)
sum(revert_data$citation_source_link_verification)
sum(revert_data$orginalResearch)
sum(revert_data$neutral)
sum(revert_data$copyright)
sum(revert_data$disambiguation)
sum(revert_data$incivility)
sum(revert_data$notable)
sum(revert_data$wordingwritingstyle)

#add groups
revert_data$groupA = -2
revert_data$groupA[which(revert_data$user_type==1)] = 1 #for group A
revert_data$groupC = -1
revert_data$groupC[which(revert_data$user_type==2)] = 1 #group B = -1; Group c = 1; group a = 0
revert_data$groupC[which(revert_data$user_type==3)] = 0

#add events
revert_data$fifa = -2
revert_data$fifa[which(revert_data$event=='fifa')] = 1 #for fifa
summary(as.factor(revert_data$fifa))
revert_data$blm = 0
revert_data$blm[which(revert_data$event=='blm')] = 1 #blm = 1; ebola = -1; fifa = 0
revert_data$blm[which(revert_data$event=='ebola')] = -1
summary(as.factor(revert_data$blm))

#vandel
summary(as.factor(revert_data$groupC))
model = glmer(as.factor(vandal) ~ #as.factor(groupA) + 
                as.factor(groupC) +   (1|userid), 
              data = revert_data, family = binomial)
summary(model)

model = glmer(as.factor(citation_source_link_verification) ~ #as.factor(groupA) 
              + as.factor(groupC) +   (1|userid), 
              data = revert_data, family = binomial)
summary(model)

summary(as.factor(revert_data$groupA))
model = glmer(as.factor(vandal) ~ as.factor(fifa) + as.factor(blm)+  (1|userid), data = revert_data[which(revert_data$groupA == 1),], family = binomial)
summary(model)

model = glmer(as.factor(citation_source_link_verification) ~ as.factor(fifa) + as.factor(blm)+  (1|userid), data = revert_data[which(revert_data$groupA == 1),], family = binomial)
summary(model)

model = glmer(as.factor(neutral) ~ as.factor(fifa) + as.factor(blm)+  (1|userid), data = revert_data[which(revert_data$groupA == 1),], family = binomial)
summary(model)


#citation
model = glmer(as.factor(citation_source_link_verification) ~ as.factor(groupA) + as.factor(groupC) +  (1|userid), 
              data = revert_data, family = binomial)
summary(model)

#neutral
model = glmer(as.factor(neutral) ~ as.factor(groupA) + as.factor(groupC) + (1|userid), 
              data = revert_data, family = binomial)
summary(model)

summary(as.factor(revert_data$orginalResearch))
model = glmer(as.factor(orginalResearch) ~ as.factor(groupA) + as.factor(groupC) +(1|userid) , 
              data = revert_data, family = binomial)
summary(model)

####
revert_data = na.omit(revert_data)
colnames(revert_data)
revert_data = revert_data[which(revert_data$check==1),]

model = glmer(vandal ~ 
                as.factor(user_type) 
              + as.factor(event) 
              + (1|userid), 
              data = revert_data, family = binomial)
summary(model)

model = glmer(citation_source_link_verification ~ 
                as.factor(user_type) 
              + as.factor(event)
              + (1|userid), 
              data = revert_data, family = binomial)
summary(model)

model = glmer(neutral ~ 
                as.factor(user_type) 
              + as.factor(event) 
              + (1|userid), 
              data = revert_data, family = binomial)
summary(model)
#event ebola and fifa more neutral
