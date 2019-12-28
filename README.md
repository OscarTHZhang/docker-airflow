# Data Pipeline: from dairy farms to AgDH
This project is built based on an existing docker image for Apache Airflow. 
The original repository is here for reference: 
[Check this out](https://github.com/puckel/docker-airflow)

Initiator and Contributor: 
Oscar Zhang 
(GitHub: [OscarTHZhang](https://github.com/OscarTHZhang), Website: [Tianhe (Oscar) Zhang](https://oscarthzhang.github.io/))

## Content
* [Description](#Description) 
* [Usage](#Usage) 
* [Data-Script Mapping](#Data-and-Script-Mapping)
* [Remote Data Source Structure](#Remote-Data-Source-Structure)
* [Running Example](#Running-Example)
* [Things Left to Do](#Things-Left-to-Do)

## Description
This project is proposed to be an interface between the data from dairy farms
and AgDH Data Center, which is the backend support for Dairy Brain and other analytics
APIs. See the illustration below: <br />
![alt text](flowchart.png "Dairy Brain Backend Overall Architecture")
 
## Usage
### Basic Setup
#### Clone the repository to your local environment
Run this command in your local terminal
```bash
$ git clone https://github.com/DairyBrain/data-pipeline
```
#### Create a new docker image from Dockerfile
First, make sure docker is installed on your local machine. 
If not or you're not familiar with Docker, refer to the [here](https://www.docker.com/get-started).
In the project directory on your local machine, run this command
```bash
$ docker build -t <tag>:<label>
```
where `<tag>` is the "name" for this image and `<label>` is default as "latest". You can then
use `$ docker images` to check if the image is created on your local machine. <br />

#### Modify Docker-Compose file to run the project 
In the docker compose file `docker-compose.yml`, change the image of webserver to whatever the label you created in the 
last step 
```bash
    webserver:
        image: cabello  # Change this line to the image <tag> you defined above
        restart: always
        depends_on: ...
        ...
```
Save the compose file and then run the project
```bash
$ docker-compose up
```
The Airflow web server is at `localhost:8080`
### PostgreSQL Setup
#### Build from .sql dump file
Contact [Steve Wangen](https://github.com/blue442) for the database dump file. 
Then, follow the [instructions](https://docs.google.com/document/d/17KATPtoOBHbVwZZ0HqmdPmrg-UV-6QlhttgVKIaiJ7Q/edit?usp=sharing)
to complete the setup of your local PostgreSQL service for the project. When restoring the database from the dump, also
pay attention to the notice below.

#### Special notice for the dump file
The code is built from a special format of the path because of the discrepancies between the actual remote drive and what 
is in a table of the database, according to the dump. Therefore, to correctly run the code, we need to change the dump file
a little bit (or you could also use SQL `UPDATE` to change the content of the database after restoring from the dump file).
Inside the dump file, find the line 
```sql
-- Data for Name: farm_datasource; Type: TABLE DATA; Schema: public; Owner: dairybrain
```
Below this line, there is a block of code that will set up the table `farm_datasource`. Replace the block with the following 
below:
```postgresql
COPY public.farm_datasource (id, farm_id, software_id, software_version, file_location, datasource_type, script_name, script_arguments, active) FROM stdin;
2	1	1	\N	larson/feedwatch/	feed	feed_data_ingest.py	\N	t
4	2	1		wangen/dairycomp/	event	event_data_ingest.py		f
6	3	\N	\N	mystic_valley/tmrtracker/	feed	feed_data_ingest.py		t
5	3	\N	\N	mystic_valley/dairycomp/	event	event_data_ingest.py		t
3	1	1	\N	test/feedwatch	feed	feed_data_ingest.py	\N	t
7	6	\N	\N	arlington/dairycomp	event	event_data_ingest.py	\N	t
\.
```
Note that all of the directory are in 2-level relative paths, because it is unclear how the file system is like in the remote server.
Therefore, for testing purposes, I used this relative path as a standard.

### Some Important Concepts
#### Volume mapping
The airflow dagbag is inside `/usr/airflow/` in the docker container. Before running the server, first map the `dag/` directory
in this project to the dagbag inside docker container; also remember to map `plugins/` and `test/` directries to the corresponding
directories in the container.

#### Dockerfile vs Docker-Compose
Dockerfile is for setting up the container structure. It is like a static configuration.
Whereas Docker-Compose is a setup for the running environment (image (static) -> container (running), such as volume mapping and port connections, 
of the image created by your Dockerfile.

#### Some useful commands
Here are some useful commands to check the container setup
```bash
# checking the running container name
$ docker ps

# access the running container file system
docker exec -it <container name> /bin/sh 
```

## Data and Script Mapping
By inspecting the database dump, the following mappings can be used for finding the correct script for given .csv data
* `dairycomp/` is ingested by `event_data_ingest.py`
* `tmrtracker` and `feedwatch` are ingested by `feed_data_ingest.py`	
* `agsource` is ingested by `agsource_data_ingest.py`
For `feed_data_ingest.py`, I have already incorporated in the project by making it from a bash program to a Python callable
function. The rest of the scripts can be found in [this repository](https://github.com/DairyBrain/ingest_scripts/tree/master/python).

## Remote Data Source Structure
Expected file structure
```bash
{farm_name}/{data_type}/{yyyy-mm-dd}/*.csv
```
Note that only *.csv files will be considered in the end.
Here are the existing file structure from the smb remote drive from [Andrew Maier](https://dairybrain.wisc.edu/staff/maier-andrew/).
Refer to him if you have any further question about this remote drive.
* arlington/agsource/, dairycomp/ -> {dates}/ -> .csv files
* haag/ -> (random structure, need to standarized, not my job)
* larson/ -> dairycomp/, extracts/, feedwatch/, grande/, smart_dairy/, smart_dairy2/ -> {dates}/ -> .csv files
* mystic_valley/ -> dairycomp/, grande/, tmrtracker/ -> {dates}/ -> .csv files
* uw-arlington/ -> feedsupervisor/ -> {dates}/ -> .csv files

## Running Example
I have made the example of data ingestion of `larson/feedwatch`. The data inside can be successfully fetched, pulled to 
DAG structures, parsed by the modified scripts, and store in the new tables in the database. This is considered as a 
success of one instance of the data sourses. I am using a subset of the `larson/feedwatch` data, which is under `test/larson/feedwatch/`
in this project. The DAG file of this pipeline is `/dags/larson_feedwatch.py`. Here are some running demos. <br />
Figure 1: Creation of new tables in the farms database
![Demo 1](demo_1.png "Creation of two new tables from feedwatch data")
<br>
<br>
Figure 2: Inside the `feedwatch_dmi_data` table
![Demo 2](demo_2.png "Details in one of the tables created")

## Things Left to Do
### Pipelines Need to be Created
There are other pipelines/ingestion needed for the dairy data
* event data ingestion (currently don't have the script matched to `event_data_ingest.py` in the ingest_scripts repository)
* agsource data ingestion (currently don't have the script matched to `agsource_data_ingest.py` in the ingest_scripts repository)
* milk data ingestion (currently don't have the matching data source)

### Running Environment Setup
Currently I am testing the code on my local machine with PostgreSQL server on localhost and a local Docker container. The
project need to be brought up to a Unix runnning environment in WID building and connecting to a real WID PostgreSQL server.
