#/**
#Team Name: Rohit Hooda, Ganavi Jayaram
#Course Information: CS 5200 
#Date: 12/08/2022
#**/
#Importing the libraries
library(XML)
library(RSQLite)
library(dplyr)
library(sjmisc)

#Connecting to the Database
dbfile = "DataMineDemo.db";
dbcon <- dbConnect(RSQLite::SQLite(),dbfile)

#Drop the tables
#sql_query <- "Drop TABLE Author"
#rs <- dbExecute(dbcon, sql_query)

#sql_query <- "Drop TABLE Journal"
#rs <- dbExecute(dbcon, sql_query)

#sql_query <- "Drop TABLE Article"
#rs <- dbExecute(dbcon, sql_query)

#sql_query <- "Drop TABLE AuthorArticleJunction"
#rs <- dbExecute(dbcon, sql_query)

#Create tables
sql_query <- "CREATE TABLE Author (authorId Integer, lastName Text, foreName Text)"
rs <- dbExecute(dbcon, sql_query)

sql_query <- "CREATE TABLE Journal (
    journalTitle Text,
    year Integer,
    month Integer,
    journalVolume Integer,
    journalIssue Integer
)"
rs <- dbExecute(dbcon, sql_query)

sql_query <- "CREATE TABLE Article (
    articleId Integer,
    articleTitle Text,
    journalId Text,
    Primary Key(articleId),
    Foreign Key (journalId) REFERENCES Journal(journalId)
)"
rs <- dbExecute(dbcon, sql_query)

sql_query <- "CREATE TABLE AuthorArticleJunction (
    articleId Integer,
    authorId Text,
    Primary Key(articleId, authorId),
    Foreign Key (articleId) REFERENCES Article(articleId),
    Foreign Key (authorId) REFERENCES Author(authorId)
)"
rs <- dbExecute(dbcon, sql_query)

#Create dataframes
author.df <- data.frame(lastName  = character(),
                        foreName = character())

journal.df <- data.frame(journalTitle = character(),
                         year = integer(),
                         month = integer())

article.df <- data.frame(articleId = integer(),
                         articleTitle = character(),
                         journalId = integer())

#Load the XML file and check if it is valid
xmlFile <- "demo2.xml"
root <- xmlParse(xmlFile, validate=T)
rootDOM <- xmlRoot(root)
size<- xmlSize(rootDOM)
print(size)

#Grab all the author child elements from the XMl and load it into a dataframe
xpathExpr <- paste("//Author")
authorList <- xpathSApply(root, xpathExpr)
authorListDF <- xmlToDataFrame(authorList)

authorListDF <<- authorListDF[,c('LastName','ForeName', 'Initials')]
author.df <<- distinct(authorListDF)
authorId <- seq(1, nrow(author.df), 1)
author.df <<- cbind(authorId, author.df)

#Fill all the NA values of appearing in the dataframe with Sentinel
author.df[is.na(author.df)] = "Sentinel"

authorId <- seq(1, nrow(authorListDF), 1)
authorListDF <<- cbind(authorId, authorListDF)

#Creating a junction table dataframe
authorJunctionDf <- data.frame(PMID = integer(), iid = integer())

#Function to check whether already exists in the dataframe 
authorLoadFunction <- function(authorListDf, PMID){
  for(i in 1:nrow(authorListDf)){
    
    lastName <- authorListDf$LastName[i]
    foreName <- authorListDf$ForeName[i]
    
    if(is.null(foreName) || is.na(foreName) ){
      foreName <- "Sentinel"
    }
    if(is.na(lastName) || is.null(lastName)){
      lastName <- "Sentinel"
    }
    
    iid <- which(author.df$LastName == lastName & author.df$ForeName == foreName)
    
    if(nrow(authorJunctionDf) == 0){
      authorJunctionDf[nrow(authorJunctionDf) + 1,] <<- list(PMID, iid) 
    }else{
      aRow <- data.frame(PMID, iid)
      authorJunctionDf <<- rbind(authorJunctionDf, aRow) 
    }
  }
}

#Extracting all the details required to fill the Article Dataframe
xpathExpr <- "//PubmedArticle"
articles <- xpathSApply(root, xpathExpr)
size <- xmlSize(articles)

#Iterating through every child element in the XML file and getting it 
#corresponding authorId, articleId, articleTitle and then populating the dataframe

for(i in 1:size){
  #Getting the ith child from the XML file
  article <- articles[[i]]
  
  #Extracting the article ID of the ith article child
  articleId <- xmlGetAttr(article, "PMID")
  
  #Extracting the authors from the author List of the ith article child
  xpathExpr <- paste("//PubmedArticle[@PMID=",articleId,"]/Article/AuthorList/Author")
  
  authorListDf<- xpathApply(article, xpathExpr)
  
  if(length(authorListDf) == 0){
    next;
  }
  authorListDf<- xmlToDataFrame(authorListDf)
  
  #Function to check whether author exists in the author.df
  authorLoadFunction(authorListDf, articleId)
}

#Extracting the all journal titles 
xpathExpr <- paste("//Title")
title <- xpathSApply(root, xpathExpr, xmlValue)

#Extracting the journal Details
xpathExpr <- paste("//JournalIssue")
journalIssueNode <- xpathSApply(root, xpathExpr)
journalIssueNode <- xmlToDataFrame(journalIssueNode)
journalVolume <- journalIssueNode$Volume
journalIssue <- journalIssueNode$Issue

#Extracting the Date
#For unifying the dates across all the journals, we decided that we will convert the 
#medline data by getting the year and month that appears in the order that we see it
#during parsing.So let's say medline date was 1975 MAR-1976 APR so we will consider
#the date as 1975 MAR. 
#We thought of converting the seasons into months by using the standard definition
#of when a specific season starts. For example, as Fall starts from Sep, so we 
#replaced every fall season entry that we encountered by the month Sep.

pubDate <- journalIssueNode[,c('PubDate')]
year <- substring(pubDate, 1, 4)
monthString <- substring(pubDate,5)   

#Filling in -1 in place of NA in the journalVolume and journalIssue
for(i in 1:length(journalVolume)){
  if(is.na(journalVolume[i])){
    journalVolume[i] <- -1
  }
  if(is.na(journalIssue[i])){
    journalIssue[i] <- -1
  }
}

#Substituing a Sentinel when the publishing date of a journal is not found
#Parsing the dates and making them obey a certain format
for(i in 1:length(monthString)){
  month <- monthString[i]
  if(nchar(month) == 0){
    month <- "Sentinel"
    monthString[i] <- month
  }else if(str_contains(month, "-")){
    month <- substring(month, 2, 4)
    monthString[i] <- month
  }
  else if(grepl("\\d", month)){
    month <- substring(month, 1,3) 
    monthString[i] <- month
  }else if(str_contains(month, "Spring")){
    monthString[i] <- "Mar"
  }else if(str_contains(month, "Fall")){
    monthString[i] <- "Sep"
  }else if(str_contains(month, "Summer")){
    monthString[i] <- "Jun"
  }else if(str_contains(month, "Winter")){
    monthString[i] <- "Dec"
  }
}

#Populating the journal.df from the parsed data from the XML
journal <- data.frame(title, year, monthString, journalIssue, journalVolume)
journal.df <-  rbind(journal.df, journal)
journal.df <<- distinct(journal.df)
journalId <- seq(1, nrow(journal.df), 1)
journal.df <<- cbind(journalId, journal.df)


#Extracting the title of article
xpathExpr <- paste("//ArticleTitle")
articleTitle <- xpathSApply(root, xpathExpr,xmlValue)
articleTitle <- data.frame(articleTitle)
articleID <- xpathSApply(root, "//PubmedArticle/@PMID")
articleId <-  as.numeric(articleID)
articleId <- data.frame(articleId)

article.df<-cbind(articleId, articleTitle, title, journalVolume, journalIssue)

# Finding a correct associating journalId for a specific article
for(i in 1:nrow(article.df)){
  journalTitleArticle <- article.df$title[i]
  journalIssueArticle <- article.df$journalIssue[i]
  journalVolumeArticle <- article.df$journalVolume[i]
  
  for(j in 1:nrow(journal.df)){
    journalTitle <- journal.df$title[j]
    journalIssue <- journal.df$journalIssue[j]
    journalVolume <- journal.df$journalVolume[j]
    
    if(journalTitleArticle == journalTitle && 
       journalIssueArticle == journalIssue && 
       journalVolumeArticle == journalVolume ){
      article.df$journalId[i] <- journal.df$journalId[j]
    }
  }
}

article.df <- article.df[, c('articleId','articleTitle','journalId')]

#Writing the data frame into their respective columns
dbWriteTable(dbcon, "Author", author.df, row.names = FALSE, overwrite = TRUE)
dbWriteTable(dbcon, "Journal", journal.df, row.names = FALSE, overwrite = TRUE)
dbWriteTable(dbcon, "Article", article.df, row.names = FALSE, overwrite = TRUE)
dbWriteTable(dbcon, "AuthorArticleJunction", authorJunctionDf, row.names = FALSE, overwrite = TRUE)

#Query to check if the data was loaded successfully in author table
sql_query <- "SELECT * from Author"
author <- dbGetQuery(dbcon, sql_query)
print(author)

#Query to check if the data was loaded successfully in journal table
sql_query <- "SELECT * from Journal"
journal <- dbGetQuery(dbcon, sql_query)
print(journal)

#Query to check if the data was loaded successfully in the Article Table
sql_query <- "SELECT * from Article"
article <- dbGetQuery(dbcon, sql_query)
print(article)

#Query to check if the data was loaded successfully in AuthorArticleJunction table
sql_query <- "SELECT * from AuthorArticleJunction"
AuthorArticleJunction <- dbGetQuery(dbcon, sql_query)
print(AuthorArticleJunction)

#Disconnecting the database connection
dbDisconnect(dbcon)
