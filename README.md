# Data Pipeline
This project is built based on an existing docker image for Apache Airflow. 
The original repository is here for reference: <br />
https://github.com/puckel/docker-airflow

Contributor: [Oscar Zhang](https://github.com/OscarTHZhang)

## Description
This project is proposed to be an interface between the data from dairy farms
and AgDH Data Center, which is the backend support for Dairy Brain and other analytics
APIs. See the illustration below: <br />
![alt text](flowchart.png "Architecture")
 
## Usage
Volume mapping:
The airflow dagbag is inside ```/usr/airflow``` in the docker container. Before running the server, first map the dag directory in this project to the dagbag inside docker container; also remember to map plugins and test directries to the corresponding directories in the container

To build from Dockerfile:
```bash
docker build -t <$tag>:<$label> .
```

To run from Docker-compose:
```bash
docker-compose up
```
This will start up the SequentialExecutor that is originally inside
```docker-compose-LocalExecutor.yml```

The Airflow web UI will be started on ```localhost:8080```. Currently,
to refresh the dags for each run, we have to manual delete the dag files inside
the dagbag, remove it from the web UI, and then add the dag file back to
the dagbag.

To check the parsing result:
```bash
# access the running container
docker exec -it <container name> /bin/sh 

# check the result in the volumed test directory in the container
$cd test
```

## Data-Script Mapping
By inspecting the database dump, the following mappings can be used for finding the correct script for given .csv data
* dairycomp -> event_data_ingest.py
* tmrtracker, feedwatch -> feed_data_ingest.py	
* agsource - > agsource_data_ingest.py

## Directory Structure
Expected file structure
```bash
{farm_name}/{data_type}/{yyyy-mm-dd}/*.csv
```
Note that only *.csv files will be considered in the end
toplevel <br>
* arlington/ -> agsource/, dairycomp/ -> {dates}/ -> .csv files
* haag/ -> (random structure, need to standarized, not my job)
* larson/ -> dairycomp/, extracts/, feedwatch/, grande/, smart_dairy/, smart_dairy2/ -> {dates}/ -> .csv files
* mystic_valley/ -> dairycomp/, grande/, tmrtracker/ -> {dates}/ -> .csv files
* uw-arlington/ -> feedsupervisor/ -> {dates}/ -> .csv files



## Todo
Connect to the ingest csv file portal and AgDH database to test and imoprove the 
dag functionality.

