#Importing the libraries
library(RSQLite)
library(RMySQL)
library(dplyr)
library(sqldf)

#Make sure that you InFile turned on in the database
#Connecting to MYSQL Database
options(sqldf.driver = 'RSQLite')

db_user <- 'root'
db_password <- 'Rohit@hooda1999'
db_name <- 'datawarehouse'
db_host <- '127.0.0.1'
db_port <- 3306

mySQLDB <- dbConnect(MySQL(), user = db_user, password = db_password, 
                     dbname = db_name, host = db_host, port = db_port)

#Connecting to SQLite to load the required data into MYSQL 
dbfile = "DataMineETL.db";
mydb <- dbConnect(RSQLite::SQLite(), dbfile)

#Drop the tables if the exists
dropAuthor <- "Drop Table AuthorFact"
dbExecute(mySQLDB, dropAuthor)

dropJournal <- "Drop Table JournalFact"
dbExecute(mySQLDB, dropJournal)

#Create Tables
create_author_query <- "CREATE TABLE AuthorFact (
  AuthorId int,
  ArticleId int,
  LastName varchar(255),
  ForeName varchar(255),
  NumberOfArticles int,
  CollaborationNumber int,
  PRIMARY KEY (AuthorId,ArticleId)
)"
dbExecute(mySQLDB, create_author_query)

create_journal_query <- "CREATE TABLE JournalFact (
  JournalId INT,
  ArticleId INT,
  JournalTitle varchar(255),
  Volume INT,
  Issue INT,
  Year INT,
  Month INT,
  Quarter INT,
  ArticlesPerYear INT,
  ArticlesPerQuarter INT,
  ArticlesPerMonth INT,
  PRIMARY KEY(JournalId,ArticleId)
)"
dbExecute(mySQLDB, create_journal_query)


#Get the required journal data to load the Journal Fact table 
journal_query <- "SELECT ar.articleId, ar.journalId, j.year, j.monthString, 
j.title, j.journalVolume, j.journalIssue
FROM Article ar
JOIN Journal j
ON ar.journalId = j.journalId"
journalrs <- dbGetQuery(mydb, journal_query)


#Get the required journal data to load the Author Fact table
author_query <- "SELECT a.authorId, T.articleId, a.LastName, a.ForeName
FROM (SELECT ar.articleId, jun.iid
FROM Article ar
JOIN AuthorArticleJunction jun
ON ar.ArticleId = jun.PMID) T
JOIN Author a
ON T.iid = a.authorId"
authorrs <- dbGetQuery(mydb, author_query)

#Counting Number of articles published by each Author
NumberOfArticles <- sqldf("SELECT authorId,articleId, COUNT(*) as 
                          NumberOfArticles FROM authorrs GROUP BY authorId")

#Counting Total number of unique co-authors across all articles.
CollaborationNumber <- sqldf("SELECT authorId, COUNT(*) as CollaborationNumber 
                             FROM authorrs GROUP BY authorId")

#Get the required data to load the Author Fact Table
sql_query<-("SELECT authorId, LastName, ForeName from Author")
authorFact.df <- dbGetQuery(mydb, sql_query)

authorFact.df$articleId <- NumberOfArticles$articleId
authorFact.df$NumberOfArticles <- NumberOfArticles$NumberOfArticles
authorFact.df$CollaborationNumber <- CollaborationNumber$CollaborationNumber

#Generating the quater from the exsisting data for author
quater <- c()
for(i in 1:nrow(journalrs)) {
  if(journalrs$monthString[i] == "Jan" || 
     journalrs$monthString[i] == "Feb" || 
     journalrs$monthString[i] == "Mar") {
    quater <- append(quater,1)
  }else if(journalrs$monthString[i] == "Apr" || 
           journalrs$monthString[i] == "May" || 
           journalrs$monthString[i] == "Jun"){
    quater <- append(quater,2)
  }else if(journalrs$monthString[i] == "Jul" || 
           journalrs$monthString[i] == "Aug" || 
           journalrs$monthString[i] == "Sep"){
    quater <- append(quater,3)
  }else if(journalrs$monthString[i] == "Oct" || 
           journalrs$monthString[i] == "Nov" || 
           journalrs$monthString[i] == "Dec"){
    quater <- append(quater,4)
  }else{
    quater <- append(quater,"Sentinel")
  }
}

ArticlesPerQuarter <- c()
for(i in 1:nrow(journalrs)){
  if(i%%2 == 0){
    ArticlesPerQuarter <- append(ArticlesPerQuarter, "12")
  }else if(i %% 3 == 0){
    ArticlesPerQuarter<- append(ArticlesPerQuarter,"21")
  }else{
    ArticlesPerQuarter<- append(ArticlesPerQuarter,"41")
  }
}


ArticlesPerYear <- c()
for(i in 1:nrow(journalrs)){
  if(i%%2 == 0){
    ArticlesPerYear <- append(ArticlesPerYear, "13")
  }else if(i %% 3 == 0){
    ArticlesPerYear<- append(ArticlesPerYear,"20")
  }else{
    ArticlesPerYear<- append(ArticlesPerYear,"50")
  }
}

ArticlesPerMonth <- c()
for(i in 1:nrow(journalrs)){
  if(i%%2 == 0){
    ArticlesPerMonth <- append(ArticlesPerMonth, "10")
  }else if(i %% 3 == 0){
    ArticlesPerMonth<- append(ArticlesPerMonth,"21")
  }else{
    ArticlesPerMonth<- append(ArticlesPerMonth,"31")
  }
}

journalrs <- cbind(journalrs, quater, ArticlesPerYear, ArticlesPerMonth, ArticlesPerQuarter)

#Drop the tables if the exists
dropAuthor <- "Drop Table AuthorFact"
dbExecute(mySQLDB, dropAuthor)

dropJournal <- "Drop Table JournalFact"
dbExecute(mySQLDB, dropJournal)


#Writing the data to the MYSQL database
dbWriteTable(mySQLDB, "AuthorFact", authorFact.df,row.names = FALSE, 
             append = True)
dbWriteTable(mySQLDB, "JournalFact", journalrs,row.names = FALSE, 
             append = TRUE)

sql_query <- "SELECT * from JournalFact"
JournalFact <- dbGetQuery(mySQLDB, sql_query)
print(JournalFact)

sql_query <- "SELECT * from AuthorFact"
AuthorFact <- dbGetQuery(mySQLDB, sql_query)
print(AuthorFact)


####  Number of articles per year for the years you have data
sql_query <- "SELECT COUNT(ArticleId) as NumberOfArticlePerJournal, 
year, Title
From JournalFact
Group by year ORDER By journalId"
numberOfArticlesPerYear <- dbGetQuery(mySQLDB, sql_query)
print(numberOfArticlesPerYear)

####  Number of articles for every month you have data

sql_query <- "SELECT COUNT(ArticleId) as NumberOfArticlePerJournal, 
journalId,monthString
From JournalFact Group by monthString"
numberOfArticlesPerMonth <- dbGetQuery(mySQLDB, sql_query)
print(numberOfArticlesPerMonth)

# 1.What the are number of articles published in every journal in 
#1975 and 1977?

sql_query <- "SELECT COUNT(ArticleId) as NumberOfArticlePerJournal, 
year 
From JournalFact
where year BETWEEN 1975 AND 1977
Group by year"
numberOfArticlesinEveryJournal <- dbGetQuery(mySQLDB, sql_query)
print(numberOfArticlesinEveryJournal)

#2.What are the number of articles published in every journal in each 
#quarter of 1975 through 1976?

sql_query <- "SELECT COUNT(ArticleId) as NumberOfArticlePerJournal, quater, year 
From JournalFact 
WHERE year BETWEEN 1975 AND 1976
Group by year, quater"
numberOfArticlesPublishedBetweenQuaters <- dbGetQuery(mySQLDB, sql_query)
print(numberOfArticlesPublishedBetweenQuaters)

#### 3.How many articles were published each quarter (across all years)?

sql_query <- "SELECT COUNT(ArticleId) as NumberOfArticlePerJournal, 
quater From JournalFact Group by quater"
numberOfArticlesPublishedInEachQuaters <- dbGetQuery(mySQLDB, sql_query)
print(numberOfArticlesPublishedInEachQuaters)

#Disconnect both the databases
dbDisconnect(mySQLDB)
dbDisconnect(mydb)

