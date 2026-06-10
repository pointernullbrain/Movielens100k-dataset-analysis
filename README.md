# 🎬 MovieLens PySpark to Cassandra Pipeline

## 📖 Project Description
This project demonstrates an end-to-end Big Data pipeline using **Apache Spark** and **Apache Cassandra**. It processes the MovieLens 100k dataset to extract user demographics, movie genres, and rating behaviors, performing complex aggregations and windowing functions before loading the final analytics into a NoSQL Cassandra database.

## 🛠️ Technologies Used
* **Apache Spark (PySpark):** Data ingestion, RDD/DataFrame transformations, and Spark SQL analytics.
* **Apache Cassandra:** NoSQL database for storing processed analytical tables.
* **JupyterLab:** Interactive development environment.
* **Linux (Hortonworks Sandbox):** Execution environment.

## 📋 Prerequisites & Environment Setup
This project bypasses Hadoop HDFS to read directly from the local Linux file system. 

1. **Download the Dataset:**
   Download and extract the MovieLens dataset to the `/tmp/` directory on your machine.
   ```bash
   wget [https://files.grouplens.org/datasets/movielens/ml-100k.zip](https://files.grouplens.org/datasets/movielens/ml-100k.zip)
   unzip ml-100k.zip -d /tmp/

2. **Start Cassandra**
   If Cassandra is installed manually via tarball, start the daemon as root:
   ```bash
   cd /opt/cassandra
   bin/cassandra -R

3. **Initialise Cassandra Keyspace and Tables:**
   Open the Cassandra shell (`/opt/cassandra/bin/cqlsh`) and execute the following to prepare the database
   ```SQL
   CREATE KEYSPACE IF NOT EXISTS movielens_analysis 
   WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

   USE movielens_analysis;

   CREATE TABLE IF NOT EXISTS movie_avg_ratings (
     movie_id INT PRIMARY KEY,
     movie_title TEXT,
     avg_rating FLOAT
    );
    
   CREATE TABLE IF NOT EXISTS user_favourite_genres (
     user_id INT PRIMARY KEY,
     favourite_genre TEXT,
     rating_count INT
    );
    
   CREATE TABLE IF NOT EXISTS filtered_users (
     user_id INT PRIMARY KEY,
     age INT,
     gender TEXT,
     occupation TEXT,
     zip_code TEXT
    );

## 🚀 Running the Pipeline
1. Ensure the Jupyter Spark session has the correct Cassandra connector package for your Spark version. ("com.datastax.spark:spark-cassandra-connector_2.12:3.X.X")
2. Run the Jupyter cells sequentially.
3. The pipeline will automatically load the local data, execute the queries, append the data to Cassandra, and print validation tables directly from the database.

## 📊 Analytical Tasks Performed
1. Task i & ii: Ranked the Top 10 movies globally based on their average rating.
2. Task iii: Identified the single favourite movie genre for "frequent raters" (users with >= 50 ratings) using data melting and row-number window functions.
3. Task iv & v: Filtered specific demographic groups (users under 20, and Scientists aged 30-40) and consolidated them into a uniform Cassandra table.

##  📝 Results
### Analysis of datasets
<img width="531" height="268" alt="image" src="https://github.com/user-attachments/assets/90bbb99e-79ae-44ab-938f-6b65de596d2c" />

<img width="466" height="271" alt="image" src="https://github.com/user-attachments/assets/39e337cc-70eb-46bc-b765-ffba7dd6f5ef" />

<img width="365" height="269" alt="image" src="https://github.com/user-attachments/assets/d0e65b91-8563-42a4-9302-711711d9370e" />

<img width="401" height="278" alt="image" src="https://github.com/user-attachments/assets/78a4bd35-3b59-4600-a91b-5e34d7cd0a9f" />

### Validation from Cassandra
<img width="376" height="626" alt="image" src="https://github.com/user-attachments/assets/e79aee6b-7d49-4142-b52f-c8cd9054270b" />


