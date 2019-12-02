DROP SCHEMA IF EXISTS smart_dairy CASCADE;
CREATE SCHEMA smart_dairy;

# permanent versions of selected smart dairy tables
# only difference is the id's are defined as primary keys, which is required for "insert on conflict update..." ie "upserts"
CREATE TABLE smart_dairy.tblcows (
    cow_id integer PRIMARY KEY,
    cow_number bigint NOT NULL,
    cow_name character varying(32),
    breedcode_id integer,
    birth_date timestamp without time zone,
    lactations integer,
    isbull boolean,
    iscomplete boolean,
    active boolean,
    tstamp timestamp without time zone DEFAULT now()
);

CREATE TABLE smart_dairy.tblmilkings (
    milking_id integer PRIMARY KEY,
    cow_id integer NOT NULL,
    milkingshift_id integer NOT NULL,
    milkingassignment_id integer NOT NULL,
    id_tag_number_assigned character varying(20),
    id_tag_read_duration bigint,
    identified_tstamp timestamp without time zone DEFAULT now(),
    id_control_address integer,
    milk_conductivity real,
    lot_number_assigned integer,
    lot_number_milked integer,
    detacher_address integer,
    ismanualcownumber boolean,
    milk_weight real,
    cow_activity integer,
    flow_rate_to_detach real,
    dumped_milk real,
    mastitis_code integer,
    tstamp timestamp without time zone DEFAULT now(),
    max_milk_temperature real DEFAULT 0
);

CREATE TABLE smart_dairy.tblattachtimes (
    attachtime_id integer PRIMARY KEY,
    milking_id integer NOT NULL,
    detacher_address integer NOT NULL,
    tstamp timestamp without time zone DEFAULT now()
);

CREATE TABLE smart_dairy.tbldetachtimes (
    detachtime_id integer PRIMARY KEY,
    milking_id integer NOT NULL,
    detacher_address integer NOT NULL,
    milk_weight real,
    tstamp timestamp without time zone DEFAULT now()
);

CREATE TABLE smart_dairy.tblcowtags (
    cowtag_id integer PRIMARY KEY,
    cow_id integer NOT NULL,
    tagtype_id integer DEFAULT 0 NOT NULL,
    tag_number character varying(20),
    is_management boolean DEFAULT false,
    active boolean DEFAULT true,
    tstamp timestamp without time zone DEFAULT now()
);

CREATE TABLE smart_dairy.tblcowlots (
    cowlot_id integer NOT NULL,
    cow_id integer NOT NULL,
    lot_id integer,
    active boolean,
    tstamp timestamp without time zone DEFAULT now()
);

CREATE TABLE smart_dairy.tblcowreprostatuses (
    cowreprostatus_id integer NOT NULL,
    cow_id integer NOT NULL,
    reprostatus_id integer,
    active boolean,
    tstamp timestamp without time zone DEFAULT now()
);
