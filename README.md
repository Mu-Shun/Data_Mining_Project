# Data_Mining_Project
Data Mining Project with Yelp data

|No.|    Main Application    |Programming|Tags|
|---|------------------------|-----------|----|
|1|[MapReduce and Spark Operation](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/MapReduce_Spark_Operation/00_Assignment1.pdf)|[Python](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/MapReduce_Spark_Operation) |`MapReduce` `Spark` `Pyspark`|
|2|[Find Frequent Itemsets](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Finding_Frequent_Itemsets/Assignment2%20(1).pdf)|[Python](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Finding_Frequent_Itemsets)| `PCY` `Apriori` `SON`|
|3|[Recommendation Systems](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Recommendation_Systems/Assignment3.pdf)|[Python](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Recommendation_Systems)|`Collaborative Filtering` `MinHash` `LSH`|
|4|[Graph Network Algorithm](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Graph_Network/Assignment4.pdf)|[Python](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Graph_Network)|`Betweenness` `Communities Detection` `Girvan-Newman Algorithm`|6.5 (python) + 0.0 (scala) / 8.0 + 0.8|
|5|[Clustering Algorithm](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Clustering_with_BFR/Assignment6.pdf)|[Python](https://github.com/Mu-Shun/Data_Mining_Project/tree/master/Clustering_with_BFR)|`K-Means` `Bradley-Fayyad-Reina (BFR) Algorithm` `NMI`|
|6|[Streaming Mining](https://github.com/Mu-Shun/Data_Mining_Project/blob/master/Streaming_Data_Mining/Assignment5.pdf)|[Python](https://github.com/Mu-Shun/Data_Mining_Project/tree/master/Streaming_Data_Mining)|`Bloom Filter` `Flajolet-Martin Algorithm` `Twitter Streaming` `Reservoir Sampling`|

#### Description
Data mining is a foundational piece of the data analytics skill set. At a high level, it allows the
analyst to discover patterns in data, and transform it into a usable product. The course will
teach data mining algorithms for analyzing very large data sets. It will have an applied focus, in
that it is meant for preparing students to utilize topics in data mining to solve real world
problems

## MapReduce and Spark Operation
Completeing tasks using spark. The goal of these tasks is to help us get familiar with Spark operations (e.g., transformations and actions) and MapReduce.

## Finding Frequent ItemsetsAssignment2
Implemented the **SON** algorithm using the Apache Spark Framework. Finding frequent itemsets in two datasets, one simulated dataset and one real-world dataset generated from Yelp dataset. The goal of this assignment is to apply the algorithms we have learned in class on large datasets more efficiently in a **distributed environment**.

## Recommendation Systems
In Assignment 3, I have completed three tasks. The goal is to be familiar with **Min-Hash, Locality Sensitive Hashing (LSH)**, and **various types of recommendation systems**.

## Graph Network Algorithm
Explored the spark GraphFrames library as well as implemented my own Girvan-Newman algorithm using the Spark Framework to **detect communities in graphs**. Also tried to deal with dataset for finding users who have a similar business taste. The goal of this assignment is to understand how to use the **Girvan-Newman algorithm** to detect communities in an efficient way within a **distributed environment**.

## Clustering Algorithm
Implementing the **K-Means and Bradley-Fayyad-Reina (BFR) algorithm**. The goal is to be familiar with clustering algorithms with various distance measurements. 

## Streaming Mining
Implemented three algorithms: **the Bloom filtering, Flajolet- Martin algorithm, and reservoir sampling**. For the first task, implemented Bloom Filtering for off-line Yelp business dataset. For the second and the third task, I have dealed with online streaming data directly. In the second task, I have implemented Flajolet-Martin algorithm with Spark Streaming library. In the third task, I have done some analysis on Twitter stream using fixed size sampling (Reservoir Sampling).



