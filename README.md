# Sparkify's DATA WAREHOUSE with AWS  S3 & Redshift

As the user base and song database keep growing, there is a need of using tools that will help the analytics team to make fast **OLAP**.

>The **ETL pipeline** copy data from **JSON** files containing users logs and songs metadata stored at **AWS S3**  into **Redshift** **Staging tables** then transform and load them to **final tables** in the same Redshift schema.

### Technologies/Tools

Following are different technologies used in this project :

* [Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html) - A fully managed, petabyte-scale data warehouse service in the cloud
* [Python 3](https://www.python.org/) - Awesome programming language, easy to learn and with awesome data science libraries and communities
* [psycopg2](https://www.psycopg.org/) - Popular PostgreSQL database adapter for the Python programming language
* [configparser](https://docs.python.org/3/library/configparser.html) - Helps to work with configuration files
*  [Jupyter](https://jupyter.org/) - Project Jupyter is a nonprofit organization created to "develop open-source software, open-standards, and services for interactive computing across dozens of programming languages". *(This project is developed using Jupyter environment as the IDE(Code Editor) but it can also work well with any other code editor such as VS Code, ..)*

### Usage
This app requires the technologies mentioned above to run.

```sh
$ #cd to the project home directory/folder
$ python3 create_tables.py
$ python3 etl.py
```

### Directory(Folder) Structure

| File/Folder | Description |
| ------ | ------ |
| dwh.cfg | Configuration file containing our Redshift credentials and S3 files path. |
| sql_queries.py | Here Different **SQL queries** (Table Creation/Destroy, SELECT, INSERT) are written and store in python variables. Our ETL process is based on SQL queries which are all found here |
| create_tables.py | A module we use to create and drop our **Amazon Redshift** tables our tables |
| etl.py | A module containing all our **ETL** functions |
| README.md | Information about the project    |


### Database Schema

#### Staging Tables
-  **staging_events**

| Column | Datatype | Constrainct | Description |
| ------ | ------ |------ | ------ |
| event_id | BIGINT |IDENTITY(1,1) PRIMARY KEY | A unique Identifier added to extracted event logs data, this key will be usefull for data transformation |
| artist_name | VARCHAR(255) | |  |
| auth | VARCHAR(50) | |  |
| user_first_name | VARCHAR(255) | |  |
| user_gender | VARCHAR(1) | |  |
| item_in_session | INTEGER | |  |
| user_last_name | VARCHAR(255) | |  |
| song_length | DOUBLE PRECISION | |  |
| user_level | VARCHAR(50) | |  |
| location | VARCHAR(255 | |  |
| method | VARCHAR(4) | |  |
| page | VARCHAR(35) | |  |
| registration | VARCHAR(50) | |  |
| session_id |BIGINT | |  |
| song_title | VARCHAR(255) | |  |
| status | SMALLINT | |  |
| ts | BIGINT | |  |
| user_agent | TEXT | |  |
| user_id | BIGINT | |  |

-  **staging_songs**

| Column | Datatype | Constrainct | Description |
| ------ | ------ |------ | ------ |
| songplay_id | BIGINT |IDENTITY(1,1) PRIMARY KEY | A unique Identifier added to extracted event logs data, this key will be usefull for data transformation |
| num_songs | INT | |  |
| artist_id | VARCHAR | |  |
| artist_latitude | DECIMAL | |  |
| artist_longitude | DECIMAL | |  |
| artist_location | VARCHAR | |  |
| artist_name | VARCHAR(255) | |  |
| song_id | VARCHAR | |  |
| title | VARCHAR(255) | |  |
| duration | DOUBLE PRECISION | |  |
| year | INT | |  |

#### Final Tables

##### Dimension Tables
-  **users**

| Column | Datatype | Constrainct | Description |
| ------ | ------ |------ | ------ |
| user_id | BIGINT |PRIMARY KEY sortkey distkey| This is a unique key representing each user and we opted for an BIGINT type because we are able to extract as a non decimal number format from the log file and as the number of user grows, BIGINT will be the right  Datatype|
| first_name | VARCHAR(255) | |  |
| last_name | VARCHAR(255) | |  |
| gender | VARCHAR(1) | |  |
| level | VARCHAR(50) | |  |

-  **songs**

| Column | Datatype | Constrainct | Description |
| ------ | ------ |------ | ------ |
| song_id | varchar |PRIMARY KEY | This is a unique key representing each song and we opted for a varchar type because we are receiving the song ID as a string of different alphabetic characters from the song metadata |
| title | VARCHAR(255) | NOT NULL|  |
| artist_id | VARCHAR | distkey |  |
| year | int |sortkey |  |
| duration | DOUBLE PRECISION | |  |

-  **artists**  *diststyle all*

| Column | Datatype | Constrainct | Description |
| ------ | ------ |------ | ------ |
| artist_id | varchar |PRIMARY KEY | This is a unique key representing each artist and we opted for a varchar type because we are receiving the artist ID as a string of different alphabetic characters from the song metadata |
| name | VARCHAR(255) |NOT NULL |  |
| location | VARCHAR(255) | | |
| latitude | DECIMAL | |  |
| longitude | DECIMAL | |  |

-  **time**  *diststyle all*

| Column | Datatype | Constrainct | Description |
| ------ | ------ |------ | ------ |
| start_time | timestamp |PRIMARY KEY | This is a  key representing each recorded time in postgres timestamp format wich is YYYY-MM-DD hh:mm:ss... |
| hour | INT |NOT NULL |  |
| day | INT |NOT NULL | |
| week | INT |NOT NULL |  |
| month | INT |NOT NULL |  |
| year | INT |NOT NULL |  |
| weekday | INT |NOT NULL |  |

##### Fact Tables
-  **songplays**

| Column | Datatype | Constrainct | Description |
| ------ | ------ |------ | ------ |
| songplay_id | BIGINT IDENTITY(1,1) |PRIMARY KEY | This is an auto increament key representing each recorded songplay, BIGSERIAL type helps us to care less about the quantity of songplay records we will store as the platform grows  |
| start_time | timestamp |sortkey |  |
| user_id | BIGINT |distkey |  |
| level | varchar | | |
| song_id | varchar | | the song id stored in artists table is of varchar data type |
| artist_id | varchar || the artist id stored in artists table is of varchar data type |
| session_id | int | |  |
| location | varchar | |  |
| user_agent | varchar | |  |

### ETL pipeline
The ELT processes are operated as follow:
-   Load json data from Amazon S3 to staging tables at Redshift using copy command
-   Insert to final redshift tables INSERT INTO SELECT

>Data Transformation and Filtering are performed in the SELECT query returning values for the INSERT

The process is used by runing **etl.py** which calls functions `load_staging_tables` to load data into the staging tables then `insert_tables` insert data to final tables

##### Process Songs Data

Songs Data are retrieved from the `staging_songs` table wich contains songs data copied from songs data json files stored in Amazon S3.

Data found in the `staging_songs` table are described in the **Database Schema** section.

-  **songs**
>Songs data consist of
>`[song_id, title, artist_id, year, duration]` extracted from `staging_songs`, 
>To ensure that we have unique songs record, we filter the `staging_songs` using the sql query stored in `distinct_songs_id` variable
-  **artists**
>Data related to artists are
>`[artist_id, artist_name, artist_location, artist_latitude, artist_longitude]` which are extracted from `staging_songs`, 
>To ensure that we have unique songs record, we filter the `staging_songs` using the sql query stored in `distinct_artists_id` variable

##### Process Log/Events Files

Events or Logs Data are retrieved from the `staging_events` table wich contains logs data copied from events data json files stored in Amazon S3.

Data found in the `staging_events` table are described in the **Database Schema** section.

-  **time**
>Time records consist of
>`[start_time, hour, day, week, month, year, weekday]` which are extracted from the timestamp `ts` column of  `staging_events`
>Since the staging table contains the timestamp in number values which represent the **Unix Timestamp**, we use a formula to convert it to **Redshift TIMESTAMP** then SELECT DISTINCT values to only store unique values

-  **users**
>Data related to users are
>`[artist_id, artist_name, artist_location, artist_latitude, artist_longitude]` which are extracted from `staging_songs`, 
>To ensure that we have unique users records and non null users id, we filter the `staging_events` using the sql query stored in `distinct_events_id_with_users_id` variable

-  **songplay** 
>Songplay records consist of
>`[start_time, user_id, level, song_id, artist_id, session_id, location, user_agent]` which are extracted from the `staging_events` with a join with `staging_songs` to get the song id, the timestamp value extraction for `start_time` is processed using the same formula as the extraction for `time` table

### Contributors
**Joël Atiamutu** *[github](https://github.com/joelatiam)  [gitlab](https://gitlab.com/joelatiam)*
