DROP SCHEMA IF EXISTS dairy_comp CASCADE;
CREATE SCHEMA dairy_comp;

-- CMD 5: "ID","BDAT","LACT","RC","PEN","Event","DIM","Date","Remark","R","T","B",
drop table if exists dairy_comp.events;
CREATE TABLE dairy_comp.events (
  id      INT,
  bdat    DATE,
  lact    INT,
  rc      INT,
  pen     INT,
  event   VARCHAR(32),
  dim     INT,
  date    DATE,
  remark  VARCHAR(32),
  r       VARCHAR(12),
  t       INT,
  b       VARCHAR(12)
 );

-- CMD1 :"ID","EID","BDAT","CBRD","CNTL","REG","DBRD","DID","DREG","SID","MGSID","EDAT","Event","DIM","Date","Remark","R","T","B",
drop table if exists dairy_comp.animals;
CREATE TABLE dairy_comp.animals (
  id      INT,
  eid     VARCHAR(25),
  bdat    DATE,
  cbrd    VARCHAR(12),
  cntl    INT,
  reg     VARCHAR(25),
  dbrd    VARCHAR(12),
  did     INT,
  dreg    VARCHAR(25),
  sid     VARCHAR(25),
  mgsir   VARCHAR(25),
  active_flag boolean default TRUE,
  deactivation_date DATE
 );


DROP TABLE IF EXISTS dairy_comp.animal_events;
CREATE TABLE dairy_comp.animal_events (
     id         INT,
     eid        VARCHAR(25),
     bdat       DATE,
     cntl       INT,
     edat       DATE,
     event      VARCHAR(64),
     dim        INT,
     date       DATE,
     remark     VARCHAR(64),
     r          VARCHAR(10),
     t          INT,
     b          VARCHAR(10)
);



GRANT USAGE
    ON SCHEMA dairy_comp
    TO ingest;

GRANT CREATE
    ON SCHEMA dairy_comp
    TO ingest;

GRANT SELECT, UPDATE, INSERT, DELETE
    ON TABLE dairy_comp.events
    TO ingest;

GRANT SELECT, UPDATE, INSERT
    ON TABLE dairy_comp.animals
    TO ingest;

GRANT SELECT, UPDATE, INSERT, DELETE
    ON TABLE dairy_comp.animal_events
    TO ingest;
