CREATE DATABASE arlington WITH
    OWNER = ingest
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

GRANT CREATE, CONNECT, TEMPORARY, TEMP
    ON DATABASE arlington
    TO ingest;
