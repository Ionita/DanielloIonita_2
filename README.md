# Systems and Architectures for Big Data: Project 2
Project for the course of Systems and Architectures for Big Data at University of Rome Tor Vergata

## Introduction to the project
http://www.ce.uniroma2.it/courses/sabd1718/projects/SABD1718_progetto2.pdf

At this link you can find the project requirements in italian. For the english translation read the project report

## Prerequisites
In order to run this project you need the following softwares to be installed on your computer:
* Apache Kafka
* Apache Flink 


## Content of this page
In this page you can find the entire Java code and the report

## Run
The source directory presentes a package called "runner". It contains all the files needed in order to run the project. It is suggested to open the project with an IDE such as Intellij. Before running the classes you have to keep in mind that Kafka must be up and running. 

before running query3 the Alphabetizer class inside preprocessing directory must be runned.
before running each class in the main directory must be present a directory called "results" with sub-directories "query_1", "query_2", "query_3".

Runner classes can take as input: 

Query1FromFile: path_of_the_file

  for example /user/x/data/friendship.dat
  
Query2FromFile: --input path_of_the_file --output path_of_result_directory

  for example --input /user/x/data/comments.dat --output /user/x/data/results
  
Query1FromFile: path_of_the_file

  for example /user/x/data/query3.txt
  

Query1Flink: none

Query2Flink: none

Query3Flink: none

Query1Kafka: path_of_the_file

  for example /user/x/data/friendship.dat
  
Query2Kafka: path_of_the_file

  for example /user/x/data/comments.dat
  
Query3Kafka: path_of_the_file

  for example /user/x/data/query3.txt
  
  
Query1Monitor: none

Query2Monitor: none

Query3Monitor: none
  

## Owners
D'Aniello Simone - Data Science and Engineering - University of Rome Tor Vergata

Ionita Marius Dragos - Data Science and Engineering - University of Rome Tor Vergata
