## Load rms package
library(survminer)
library(survival)
library(KMsurv)
library(igraph)
library(reldist)
require(lme4)
#Prepare Data
setwd("/Users/angli/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
setwd("/Users/ANG/OneDrive/Documents/Pitt_PhD/ResearchProjects/Wiki_Event/data")
######################
# prepare talk network
######################

all_ids = read.csv("talk_nodes.csv") #18338 in total
all_edge = read.csv("talk_edges.csv") 
#session_edge = all_edge
colnames(all_ids)
colnames(all_edge)

#session_edge = all_edge
talk_net <- function(session_edge){
  #this function is to general the network
  session_net = graph.edgelist(cbind(session_edge$source, session_edge$target), directed=TRUE) 
  fullEdges = length(E(session_net))
  V(session_net)$wpid = as.character(all_ids$wpid)
  V(session_net)$label = as.character(all_ids$label)
  V(session_net)$id = as.character(all_ids$ID)
  V(session_net)$first_edit = as.character(all_ids$first_edit)
  V(session_net)$from_event = all_ids$from_event
  V(session_net)$final_check = all_ids$final_check
  #session1: IGRAPH D-W- 16287 17321 --
  E(session_net)$weight = session_edge$weight
  
  #results = session_net
  return (session_net)
}

#results for talk network

session_all.net = talk_net(all_edge)
session_all.net = simplify(session_all.net, remove.multiple = FALSE, remove.loops = TRUE) #remove self loop
session_all.net = delete_vertices(session_all.net, which(V(session_all.net)$final_check==0))
V(session_all.net)$degree = degree(session_all.net, mode="all",normalized = FALSE)
session_all.net = delete_vertices(session_all.net, which(V(session_all.net)$degree==0 & V(session_all.net)$from_event==0))

summary(session_all.net) 
#IGRAPH D-W- 1991 1801 -- 
#+ attr: wpid (v/c), label (v/c), id (v/c), first_edit (v/c), from_event (v/c), weight (e/n)

write_graph(session_all.net, "event_talk_net.graphmlphml", format ="graphml")
#write_graph(session1_student_edge.net, "session1_student_edge_20152016_0523.graphml", format ="graphml")

edge.net = session_all.net
mean(transitivity(edge.net, type="local"), na.rm = TRUE)
mean(strength(edge.net, mode = "total"), na.rm = TRUE)
mean(eigen_centrality(edge.net, weights = E(edge.net)$weight)$vector, na.rm = TRUE)

#network measures
network_output <- function(edge.net){
  #this function is to calculate the individual network measures
  #edge.net = session1_wiki_edge.net
  wpid = V(edge.net)$wpid
  label = V(edge.net)$label
  ID =  V(edge.net)$id
  first_edit =  V(edge.net)$first_edit
  from_event =  V(edge.net)$from_event
  transitivity = transitivity(edge.net, type="local")
  betweenness = betweenness(edge.net,weights = E(edge.net)$weight, normalized = FALSE)
  betweenness_norm = betweenness(edge.net,weights = E(edge.net)$weight, normalized = TRUE)
  closeness = closeness(edge.net,weights = E(edge.net)$weight, normalized = FALSE)
  closeness_norm = closeness(edge.net,weights = E(edge.net)$weight, normalized = TRUE)
  degree = degree(edge.net, mode="all",normalized = FALSE)
  degree_norm = degree(edge.net, mode="all",normalized = TRUE)
  Indegree = degree(edge.net, mode="in",normalized = FALSE)
  Indegree_norm = degree(edge.net, mode="in",normalized = TRUE)
  Outdegree = degree(edge.net, mode="out",normalized = FALSE)
  Outdegree_norm = degree(edge.net, mode="out",normalized = TRUE)
  weighted_degree = strength(edge.net, mode = "all", weights = E(edge.net)$weight)
  weighted_Indegree = strength(edge.net, mode = "in", weights = E(edge.net)$weight)
  weighted_Outdegree = strength(edge.net, mode = "out", weights = E(edge.net)$weight)
  eigen = eigen_centrality(edge.net, weights = E(edge.net)$weight)$vector
  net_measure = cbind(ID, label, wpid, first_edit, from_event, transitivity, 
                      betweenness, betweenness_norm, 
                      closeness, closeness_norm, degree, degree_norm, Indegree, Indegree_norm, 
                      Outdegree, Outdegree_norm, weighted_degree, weighted_Indegree, weighted_Outdegree, eigen) 
  return (net_measure)
}


all_net_measure = data.frame(network_output(edge.net))
colnames(all_net_measure)
colnames(all_net_measure) = c("ID", "label", "wpid", "first_edit", "from_event", "transitivity", 
                              "betweenness", "betweenness_norm", 
                              "closeness", "closeness_norm", "degree", "degree_norm", "Indegree", "Indegree_norm", 
                              "Outdegree", "Outdegree_norm", "weighted_degree","weighted_Indegree", "weighted_Outdegree", "eigen")

write.csv(all_net_measure, "talk_network_measure_data.csv", na = "",row.names = FALSE)

####analysis
all_net_measure = read.csv("talk_network_measure_data.csv")
colnames(all_net_measure)

event_net = all_net_measure[which(all_net_measure$from_event==1),]
event_net$groupA = -2
event_net$groupA[which(event_net$first_edit==1)] = 1 #for group A
event_net$groupC = 0
event_net$groupC[which(event_net$first_edit==2)] = -1 #group B = -1; Group c = 1; group a = 0
event_net$groupC[which(event_net$first_edit==3)] = 1

summary(as.factor(event_net$groupA))
summary(as.factor(event_net$groupC))
summary(as.factor(event_net$from_event))
summary(as.factor(event_net$event))

require(lme4)
mod1 <- lmer(closeness_norm ~ as.factor(groupA) + as.factor(groupC) + (1|event), data = event_net)
summary(mod1)

mod1 <- lmer(betweenness_norm ~ as.factor(groupA) + as.factor(groupC) + (1|event), data = event_net)
summary(mod1)

mod1 <- lmer(weighted_Indegree ~ as.factor(groupA) + as.factor(groupC) + (1|event), data = event_net)
summary(mod1)

mod1 <- lmer(weighted_degree ~ as.factor(groupA) + as.factor(groupC) + (1|event), data = event_net)
summary(mod1)

mod1 <- lmer(eigen ~ as.factor(groupA) + as.factor(groupC) + (1|event), data = event_net)
summary(mod1)

mod1 <- lmer(transitivity ~ as.factor(groupA) + as.factor(groupC) + (1|event), data = event_net)
summary(mod1)


mean(all_net_measure$transitivity[which(all_net_measure$first_edit==1)], na.rm = TRUE)
mean(all_net_measure$transitivity[which(all_net_measure$first_edit==2)], na.rm = TRUE)
mean(all_net_measure$transitivity[which(all_net_measure$first_edit==3)], na.rm = TRUE)


mean(all_net_measure$betweenness_norm[which(all_net_measure$first_edit==1)])
mean(all_net_measure$betweenness_norm[which(all_net_measure$first_edit==2)])
mean(all_net_measure$betweenness_norm[which(all_net_measure$first_edit==3)])

mean(all_net_measure$closeness_norm[which(all_net_measure$first_edit==1)])
mean(all_net_measure$closeness_norm[which(all_net_measure$first_edit==2)])
mean(all_net_measure$closeness_norm[which(all_net_measure$first_edit==3)])

mean(all_net_measure$eigen[which(all_net_measure$first_edit==1)])
mean(all_net_measure$eigen[which(all_net_measure$first_edit==2)])
mean(all_net_measure$eigen[which(all_net_measure$first_edit==3)])

mean(all_net_measure$Indegree[which(all_net_measure$first_edit==1)])
mean(all_net_measure$Indegree[which(all_net_measure$first_edit==2)])
mean(all_net_measure$Indegree[which(all_net_measure$first_edit==3)])

mean(all_net_measure$weighted_Indegree[which(all_net_measure$first_edit==1)])
mean(all_net_measure$weighted_Indegree[which(all_net_measure$first_edit==2)])
mean(all_net_measure$weighted_Indegree[which(all_net_measure$first_edit==3)])

mean(all_net_measure$Outdegree[which(all_net_measure$first_edit==1)])
mean(all_net_measure$Outdegree[which(all_net_measure$first_edit==2)])
mean(all_net_measure$Outdegree[which(all_net_measure$first_edit==3)])


mean(all_net_measure$weighted_Outdegree[which(all_net_measure$first_edit==1)])
mean(all_net_measure$weighted_Outdegree[which(all_net_measure$first_edit==2)])
mean(all_net_measure$weighted_Outdegree[which(all_net_measure$first_edit==3)])
