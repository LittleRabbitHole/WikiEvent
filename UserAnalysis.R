setwd("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
setwd("/Users/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
require(lme4)
library(survminer)
library(survival)
library(KMsurv)
library(coxme)

#retention
data = read.csv("all_survival_final_filtered20.csv")
colnames(data)
#data = data[which(data$event!="FIFA"),]
table(data$event, data$first_edit_type3)
newcomers_list = data[c("user","userid")]

data$SurvObj <- with(data, Surv(time, death == 1))
colnames(data)

data$groupA = -1
data$groupA[which(data$first_edit_type3==1)] = 2 #for group A
data$groupC = 0
data$groupC[which(data$first_edit_type3==2)] = -1 #group B = -1; Group c = 1; group a = 0
data$groupC[which(data$first_edit_type3==3)] = 1

summary(as.factor(data$groupA))
summary(as.factor(data$groupC))


model <- coxph(SurvObj ~ as.factor(groupA) + as.factor(groupC)
               + cluster(event) + cluster(groupA), 
               data = data)
summary(model) 

#edit quality
#this is the original/editing history
data = read.csv("newcomers_contri_quality.csv")
data = merge(data, newcomers_list, by = c("userid", "user"))
data  = data[!duplicated(data), ]
data$groupA = -1
data$groupA[which(data$user_type==1)] = 2 #for group A
data$groupC = 0
data$groupC[which(data$user_type==2)] = -1 #group B = -1; Group c = 1; group a = 0
data$groupC[which(data$user_type==3)] = 1

colnames(data)
summary(data$event)

model = lmer(prob.true ~ as.factor(groupA) + as.factor(groupC) + (1|event) + (1|userid), data = data)
summary(model)

data_BLM = data[which(data$event=='blm'),] 
model = lmer(prob.true ~ as.factor(groupA) + as.factor(groupC) + (1|userid), data = data_BLM)
summary(model)


###revert
data = read.csv("newcomers_contri_revert.csv")

data = data[which(data$reverted > -1),]
data = merge(data, newcomers_list, by = c("userid", "user"))
data  = data[!duplicated(data), ]

#revert summary
colnames(data)
length(unique(data$userid)) #504 event newcomers
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
