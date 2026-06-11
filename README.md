# 🎬 MovieLens PySpark to Cassandra Pipeline

## 📖 Project Description
This project demonstrates an end-to-end Big Data pipeline using **Apache Spark** and **Apache Cassandra**. It processes the MovieLens 100k dataset to extract user demographics, movie genres, and rating behaviors, performing complex aggregations and windowing functions before loading the final analytics into a NoSQL Cassandra database.

## 🖥️ Virtual Machine & Architecture Overview
To mimic a real-world production deployment, this entire pipeline runs inside an isolated enterprise Linux environment hosted on a **Virtual Machine (Hortonworks Sandbox VM)**. 

Because the virtual machine operates in an isolated network, the services are coordinated and accessed using the following architecture:

```text
[ Windows Host Browser ] 
         │
    (SSH Tunnel / Port 8888)
         ▼
 ┌────────────────────────────────────────────────────────┐
 │ Virtual Machine (Hortonworks Sandbox Linux)            │
 │                                                        │
 │  ┌────────────────┐     Reads From    ┌─────────────┐  │
 │  │   JupyterLab   │ ────────────────> │ Local Files │  │
 │  │(PySpark Engine)│                   │  (/tmp/)    │  │
 │  └────────────────┘                   └─────────────┘  │
 │           │                                            │
 │     Writes via JDBC                                    │
 │     (Localhost:127.0.0.1)                              │
 │           ▼                                            │
 │  ┌────────────────┐                                    │
 │  │   Cassandra    │                                    │
 │  │   Database     │                                    │
 │  └────────────────┘                                    │
 └────────────────────────────────────────────────────────┘
```
## 🛠️ Technologies Used
* **Apache Spark (PySpark):** Data ingestion, RDD/DataFrame transformations, and Spark SQL analytics.
* **Apache Cassandra:** NoSQL database for storing processed analytical tables.
* **JupyterLab:** Interactive development environment.
* **Linux (Hortonworks Sandbox):** Execution environment.

## 📋 Prerequisites & Environment Setup
This project bypasses Hadoop HDFS to read directly from the local Linux file system. Frequent namenode crashes happen when the files are put into HDFS. This ensures that spark is more stable and guarantees it will not crash due to network errors.

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

## 💿 How to install Cassandra manually (if you don't already have it installed)
   1. Remove broken Cassandra repo (important)
         - ```bash
            ls /etc/yum.repos.d/
         - If you see `datastax.repo` and `cassandra.repo`, remove it by running the bash command:
           ```bash
            rm -f /etc/yum.repos.d/*cassandra*
            rm -f /etc/yum.repos.d/datastax.repo
   2. Clean yum completely
         - ```bash
           yum clean all
           rm -rf /var/cache/yum
   3. Stop using yum for Cassandra. Instead use do this:
         - Move to correct folder
           ```bash
           cd /opt
         - Download Cassandra
           ```bash
           wget https://archive.apache.org/dist/cassandra/3.11.16/apache-cassandra-3.11.16-bin.tar.gz
         - Extract
           ```bash
           tar -xvzf apache-cassandra-3.11.16-bin.tar.gz
         - Rename folder
           ```bash
           mv apache-cassandra-3.11.16 cassandra
         - Start Cassandra
           ```bash
           cd cassandra
           bin/cassandra -R
         - Run Cassandra
           ```bash
           /opt/cassandra/bin/cqlsh
         - If it opens, you'll see
           ```bash
           cqlsh>
           
## 🌌 How to install JupyterLab (inside the VM) using puTTY
### **Part 1:**
   1. Install JupyterLab
      ```bash
      pip install jupyterlab
   2. Launch JupyterLab with specific flags because Jupyter looks for a desktop browser instead of using the root user in Linux
      ```bash
      jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
   3. JupyterLab will generate a long URL with a unique security token at the bottom of the screen (e.g., http://127.0.0.1:8888/?token=abc123xyz...). Keep this PuTTY window active.

### **Part 2:**
   1. Open a brand-new PuTTY configuration window.
   2. In the left-hand sidebar, navigate down to: Connection ➔ SSH ➔ Tunnels.
   3. Set up the port redirection rules in the "Port forwarding" section:
      - Source port: Type `8888` (This is the port you will type into your Windows browser).
      - Destination: Type `localhost:8888` (This tells PuTTY to forward the traffic straight to Jupyter inside the VM).
   4. Click the Add button. You will see `L8888 localhost:8888` appear in the Forwarded ports box.
   5. Scroll back up to the Session category at the very top of the left sidebar.
   6. Enter your Sandbox connection details:
         - Host Name (or IP address): Enter your VM's IP address (e.g., 127.0.0.1 or 192.168.56.101).
         - Port: Enter your standard SSH port (usually 22, or 2222 if using localhost forwarding on Hortonworks).
   7. Click Open and log in with your root credentials.

### **Part 3:**
   1. Leave both PuTTY windows running in the background (one keeping the Jupyter process alive, and the other maintaining the tunnel link).
   2. Open Google Chrome or any browser on your normal Windows desktop.
   3. Look at the token URL printed inside your first PuTTY screen, copy it, and paste it into your browser.
   4. If it doesn't open right away, simply navigate to:
      ```plaintext
      http://localhost:8888/
   5. When prompted for a password or token, copy-paste the long string of letters and numbers (`token=...`) generated by the terminal in Part 1, Step 3.
