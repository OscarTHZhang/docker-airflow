# Create the tables

CREATE TABLE farms (
  farm_id           SERIAL          PRIMARY KEY,
  name              VARCHAR(50)     NOT NULL,
  owner             VARCHAR(25)     NOT NULL,
  address           TEXT,
  phone             BIGINT,
  create_date       DATE            NOT NULL DEFAULT CURRENT_DATE,
  dialect           TEXT            DEFAULT 'postgres',
  ingest_user       TEXT,
  host              TEXT            DEFAULT 'dairy-db-2.discovery.wisc.edu',
  port              INTEGER         DEFAULT 5432,
  database          TEXT
);


CREATE TABLE software (
  id                SERIAL          PRIMARY KEY,
  name              TEXT,
  vendor            TEXT
);


-- CREATE TABLE datasource_type (
--   id                SERIAL          PRIMARY KEY,
--   name              TEXT,
--   description       TEXT
-- );


CREATE TABLE farm_datasource (
  id                SERIAL          PRIMARY KEY,
  farm_id           INTEGER         REFERENCES farms,
  software_id       INTEGER         REFERENCES software,
  software_version  TEXT,
  file_location     TEXT,
  datasource_type   TEXT,
  script_name       TEXT,
  script_arguments  TEXT,
  active            BOOLEAN
);

    
CREATE TABLE ingest_log (
  id                    SERIAL      PRIMARY KEY,
  farm_id               INTEGER     REFERENCES farms,
  farm_datasource_id    INTEGER     REFERENCES farm_datasource,
  run_date              DATE        DEFAULT CURRENT_DATE,
  ingest_file_name      TEXT,
  export_date           DATE,
  message               TEXT,
  success               BOOLEAN                
);

CREATE TABLE farm_ingest_status (
  farm_id               INTEGER     REFERENCES farms,
  active                BOOLEAN
);


# Start with some data

INSERT INTO farms (name, owner, ingest_user, database) VALUES ('Larson Acres', 'Ed Larson', 'ingest_user', 'larson');
INSERT INTO software (name, vendor) VALUES ('DairyCOMP 305', 'Valley Ag Software');
-- INSERT INTO datasource_type (name, description) VALUES ('event data', 'Event data as recorded by farm workers');
INSERT INTO farm_datasource (farm_id, software_id, software_version, file_location, datasource_type, script_name) VALUES (1, 1, NULL, '/mnt/nfs/dairy/larson/dairycomp/', 'event', 'event_data_ingest.py');

INSERT INTO farms (name, owner, ingest_user, database) VALUES ('Wangen Farms', 'Steve Wangen', 'ingest_user', 'wangen');
INSERT INTO software (name, vendor) VALUES ('FeedWatch', 'DairyOne');
INSERT INTO farm_datasource (farm_id, software_id, software_version, file_location, datasource_type, script_name) VALUES (1, 1, NULL, '/Users/stevewangen/projects/cows_20_20/data_ingest/file_monitor/test/feedwatch', 'feed', 'feed_data_ingest.py');

INSERT INTO farm_status (farm_id, active) VALUES (1, 'true');
INSERT INTO farm_status (farm_id, active) VALUES (2, 'false');

# NEAT:
  location          VARCHAR(25)
    CHECK (
      location IN ('north', 'south', 'west', 'east')
    ),
