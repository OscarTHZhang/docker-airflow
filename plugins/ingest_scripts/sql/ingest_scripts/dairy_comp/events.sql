################################
# Each load
# drop and recreate temp import and staging tables
drop table if exists dairy_comp.temp_import_events;
CREATE TABLE dairy_comp.temp_import_events (
  id      INT,
  bdat    VARCHAR(12),
  lact    INT,
  rc      INT,
  pen     INT,
  event   VARCHAR(32),
  dim     INT,
  date    VARCHAR(12),
  remark  VARCHAR(32),
  r       VARCHAR(12),
  t       INT,
  b       VARCHAR(12),
  junk    VARCHAR(12)
 );

drop table if exists dairy_comp.temp_import_breeding_events;
CREATE TABLE dairy_comp.temp_import_breeding_events (
  id      INT,
  bdat    VARCHAR(12),
  lact    INT,
  rc      INT,
  pen     INT,
  event   VARCHAR(32),
  dim     INT,
  date    VARCHAR(12),
  remark  VARCHAR(32),
  r       VARCHAR(12),
  t       INT,
  b       VARCHAR(12),
  junk    VARCHAR(12)
 );

drop table if exists dairy_comp.temp_staging_events;
CREATE TABLE dairy_comp.temp_staging_events (
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

# Load from CSV
# linux version
copy dairy_comp.temp_import_events          from '/path/to/csv/LA_CMD5_2018-03-08.CSV' DELIMITERS ',' HEADER CSV;
copy dairy_comp.temp_import_breeding_events from '/path/to/csv/LA_CMD6_2018-03-08.CSV' DELIMITERS ',' HEADER CSV;

# Windows version
COPY dairy_comp.temp_import_events          from E'C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\PostgreSQL 10\\import\\LA_CMD5_2018-03-08.CSV' DELIMITERS ',' HEADER CSV;
COPY dairy_comp.temp_import_breeding_events from E'C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\PostgreSQL 10\\import\\LA_CMD6_2018-03-08.CSV' DELIMITERS ',' HEADER CSV;


###################
# Move last week to staging table
# Collapse exact duplicates with "select distinct"
# Convert dates to date type. Trim whitespace (leading & trailing) from text fields.
# regular events
insert into dairy_comp.temp_staging_events (id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b)
    select distinct id, to_date(bdat,'MM/DD/YY'), lact, rc, pen, trim(event), dim, to_date(date,'MM/DD/YY'), trim(remark), trim(r), t, trim(b)
    from dairy_comp.temp_import_events
    where to_date(date,'MM/DD/YY') > ((select max(to_date(date,'MM/DD/YY')) from dairy_comp.temp_import_events) - integer '7');
# breeding events
insert into dairy_comp.temp_staging_events (id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b)
    select distinct id, to_date(bdat,'MM/DD/YY'), lact, rc, pen, trim(event), dim, to_date(date,'MM/DD/YY'), trim(remark), trim(r), t, trim(b)
    from dairy_comp.temp_import_breeding_events
    where to_date(date,'MM/DD/YY') > ((select max(to_date(date,'MM/DD/YY')) from dairy_comp.temp_import_events) - integer '7');

# Delete days from events table
delete from dairy_comp.events where date in (select distinct date from dairy_comp.temp_staging_events);

# insert
insert into dairy_comp.events (id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b)
    select id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b
    from dairy_comp.temp_staging_events;

