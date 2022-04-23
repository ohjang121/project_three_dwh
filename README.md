# Project 3: Data Warehouse

Objective: Build an ETL pipeline for a music app (Sparkify) using Python & Redshift. Two datasets used are both in JSON format, and key parts are extracted from the datasets to build a star schema optimized for queries on song play analysis.

## Schema & Table Design

Data are stored in AWS S3:
* Song data: `s3://udacity-dend/song_data`

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. Example file:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

* Log data: `s3://udacity-dend/log_data`

The second dataset consists of log files in JSON format generated by this **[event simulator](https://github.com/Interana/eventsim)** based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are **partitioned by year and month**. Example data:
```
{"artist":"The Rakes","auth":"Logged In","firstName":"Isaac","gender":"M","itemInSession":2,"lastName":"Valdez","length":150.59546,"level":"free","location":"Saginaw, MI","method":"PUT","page":"NextSong","registration":1541078099796.0,"sessionId":112,"song":"Strasbourg","status":200,"ts":1541191397796,"userAgent":"Mozilla\/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko\/20100101 Firefox\/31.0","userId":"3"}
```

The database name is `sparkify`. Before transforming and loading the data into the fact table and dimensional tables, we store them temporarily in 2 staging tables - `staging_events` (log_data) and `staging_songs` (song_data). This enhances the ETL process by easing the data transform and loading process from an existing table instead of performing those actions directly from S3.

The project's star schema contains 1 fact table and 4 dimension tables - create, insert, and drop SQL logic defined in `sql_queries.py`.

### Fact Table(s)

![sparkifydb_erd](https://user-images.githubusercontent.com/49255961/162268146-fd285a30-5750-45ad-b642-b04a8fceeb72.png)

1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`
    - columns: *songplay_id (pkey), start_time (foreign key to **time**), user_id (foreign key to **users**), level, song_id (foreign key to **songs**), artist_id (foreign key to **artists**), session_id, location, user_agent*
    
### Dimension Table(s)

1. **users** - users in the app
    - columns: *user_id (pkey), first_name, last_name, gender, level*
2. **songs** - songs in music database
    - columns: *song_id (pkey), title, artist_id, year, duration*
3. **artists** - artists in music database
    - columns: *artist_id (pkey), name, location, latitude, longitude*
4. **time** - timestamps of records in **songplays** broken down into specific units
    - columns: *start_time (pkey), hour, day, week, month, year, weekday*
    
`create_tables.py` creates and connects to the `sparkify` database to build the shell tables so that data can be loaded into the tables.

Note: we use `psycopg2`, PostgreSQL python driver, to connect to RedShift since they are compatible.

## AWS set up

Before proceeding with the ETL process, we must set up an IAM Role & RedShift Cluster using a pre-defined IAM User (where AWS crendentials are obtained from).

`aws_setup.py` contains logic to both create an IAM Role (with S3 Readonly access) & a RedShift Cluister and deleting them. It prints the cluster endpoint (host input for psycopg2 connection) and IAM role ARN (input for COPY commands for staging tables). Insert them in the appropriate key-value pair in the `dwh.cfg` file.

In order to run the deletion logic, run the script with the `--delete` flag like this: `python aws_setup.py --delete`.

## ETL Process

1. The Project Workplace already has necessary libraries installed, but for a brand new environment, it is required to install all libraries that are not built-in (e.g. psycopg2).
2. Run `create_tables.py` to create shell tables for both the staging and fact / dimensional tables: 
```
python create_tables.py
```
3. Run `etl.py` to perform the ETL Process - this takes about a couple of hours with the data transforming and loading.
```
python etl.py
```

Once finished, you can check out the newly created tables in AWS RedShift query editer!

## Example Queries

* How many distinct users are Male?

`select count(distinct user_id) from users where gender = 'M'`

* List mobile user_agents

`select user_agent from songplays where user_agent ilike '%mobile%' group by 1`

## Additional Considerations

* AWS set up: I decided to create logic for AWS IAM role & cluster creation. I included the deletion logic in the same script to minimize redundant code in setting up the AWS client in the scripts.
* Distribution / Sorting keys: The datasets were not too big, so I decided to leave the dist style auto (default), which would treat small tables to be all and large to be even. I added sortkeys on timestamp fields that would be used frequently for analytical purposes.
* Additional commands (e.g. cascade, truncatecolumns): Additional parameters / columns were used to ensure that all queries are performed without being dependent on each other (cascade) and bad data are taken out (e.g. artist_name exceeding varchar (256)).


