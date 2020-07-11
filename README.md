# ETL SPARKIFY <br>
This project provides an ETL pipeline to populate a data warehouse in the cloud for the Sparkify team.

The ETL and data warehouse are built on the AWS cloud. Utilizing python, the script copys data from JSON documents in a S3 bucket to staging tables hosted on Amazon Redshift, then the data is arranged into a star schema to allow the Sparkify team the ability to run queries and analyze song and log user data.

## Quick Start <br>
First, fill in AWS acces key (KEY) and secret (SECRET).

To access AWS, you need to do in AWS the following:

create IAM user (e.g. myRedshiftRole)
create IAM role (e.g. dwhRole) with AmazonS3ReadOnlyAccess access rights
get ARN
For running cluster, you can use Udacity-DEND-Project-3.ipynb.Or you can set it up manually.

python3 create_tables.py (to create the DB to AWS Redshift)
python3 etl.py (to process all the input data to the DB)

You will need to create a configuration file with the file name dwh.cfg and the following structure:

[CLUSTER]
HOST=<your_host>  #You will get this when running the Udacity-DEND-Project-3.ipynb
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_PORT=<your_db_port>
DB_REGION=<your_db_region>
CLUSTER_IDENTIFIER=<your_cluster_identifier>

[IAM_ROLE]
ARN=<your_iam_role_arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
ACCESS_KEY=<your_access_key>
SECRET_KEY=<your_secret_key>

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large

DWH_IAM_ROLE_NAME=<your_iam_role>
DWH_CLUSTER_IDENTIFIER=<your_cluster_identifier>
DWH_DB=<your_db<
DWH_DB_USER=<your_db_user>
DWH_DB_PASSWORD=<your_db_password>
DWH_PORT=5439

[BUCKET]
BUCKET=udacity-dend
<br>

## Structure <br>
The project contains the following components:

create_tables.py creates staging tables and star schema tables in Redshift.
etl.py defines the ETL pipeline by extracting data from S3 andloading into staging tables on Redshift, and then inserting data into the star schema.
sql_queries.py defines the SQL queries that create the staging tables, the star schema and ETL pipeline
Udacity-DEND-Project-3.ipynb optional use to launch a cluster, get the host name and run create_tables.py and etl.py scripts. Helps query against tables as well.
<br>


## Schema <br>


Staging tables
staging_events: event data of user activity
staging_songs: song data about songs and artist.

Fact Table
songplays: song play data together with user, artist, and song info
Dimension Tables
users: user info
songs: song info
artists: artist info
time: detailed time info about song plays
<br>

## Build Status <br>

 The project has been completed and ready for deployment. <br>

 ## Code Style <br>

 Standard. <br>
