drop table if exists dairy_comp.temp_import_animals;
CREATE TABLE dairy_comp.temp_import_animals (
  id      INT,
  eid     VARCHAR(25),
  bdat    VARCHAR(25),
  cbrd    VARCHAR(12),
  cntl    INT,
  reg     VARCHAR(25),
  dbrd    VARCHAR(12),
  did     INT,
  dreg    VARCHAR(25),
  sid     VARCHAR(25),
  mgsir   VARCHAR(25),
  edat    VARCHAR(25),
  event   VARCHAR(25),
  dim     INT,
  date    VARCHAR(12),
  remark  VARCHAR(32),
  r       VARCHAR(12),
  t       INT,
  b       VARCHAR(12),
  trailing_comma VARCHAR(12)
 );

drop table if exists dairy_comp.temp_staging_animals;
CREATE TABLE dairy_comp.temp_staging_animals (
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
  mgsir   VARCHAR(25)
 );

-- Load the data into a database table.
-- This can and probably should be replaced by the python insert command because this will error completely if even a single row is bad.
COPY dairy_comp.temp_import_animals          from E'C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\PostgreSQL 10\\import\\LA_CMD1_2018-03-08.CSV' DELIMITERS ',' HEADER CSV;

-- Copy distinct animals to staging
-- id, eid, & bdat uniquely identifies animal
-- Unclear if any of these other fields are needed.
insert into dairy_comp.temp_staging_animals (id, eid, bdat, cbrd, cntl, reg, dbrd, did, dreg, sid, mgsir)
  select id, trim(eid), to_date(bdat,'MM/DD/YY'), max(trim(cbrd)), max(cntl), max(trim(reg)), max(trim(dbrd)), max(did), max(trim(dreg)), max(trim(sid)), max(trim(mgsir))
    from dairy_comp.temp_import_animals
   group by id, eid, bdat;

-- Copy animals from staging if doesn't already exist in animals table
insert into dairy_comp.animals (id, eid, bdat, cbrd, cntl, reg, dbrd, did, dreg, sid, mgsir)
  select id, eid, bdat, cbrd, cntl, reg, dbrd, did, dreg, sid, mgsir
    from dairy_comp.temp_staging_animals t
   where not exists (select * from dairy_comp.animals where id = t.id and eid = t.eid and bdat = t.bdat);


##
## Active Animals
drop table if exists dairy_comp.temp_import_active_animals;
CREATE TABLE dairy_comp.temp_import_active_animals (
  id      INT,
  eid     VARCHAR(25),
  bdat    VARCHAR(25),
  cbrd    VARCHAR(12),
  cntl    INT,
  reg     VARCHAR(25),
  dbrd    VARCHAR(12),
  did     INT,
  dreg    VARCHAR(25),
  sid     VARCHAR(25),
  mgsir   VARCHAR(25),
  edat    VARCHAR(25),
  trailing_comma VARCHAR(12)
 );

drop table if exists dairy_comp.temp_staging_active_animals;
CREATE TABLE dairy_comp.temp_staging_active_animals (
  id      INT,
  eid     VARCHAR(25),
  bdat    DATE
 );

COPY dairy_comp.temp_import_active_animals          from E'C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\PostgreSQL 10\\import\\LA_CMD2_2018-03-08.CSV' DELIMITERS ',' HEADER CSV;

-- Copy distinct animals to staging
insert into dairy_comp.temp_staging_active_animals (id, eid, bdat)
  select distinct id, trim(eid), to_date(bdat,'MM/DD/YY')
    from dairy_comp.temp_import_active_animals
   where trim(bdat) != '-';

-- If animal not in this new active animal file, then set active_flag and deactivation_date
update dairy_comp.animals a
   set a.active_flag = FALSE,
       a.deactivation_date = current_date
 where a.active_flag = TRUE
   and not exists (select * from dairy_comp.temp_staging_active_animals t where t.id = a.id and t.eid = a.eid and t.bdat = a.bdat);