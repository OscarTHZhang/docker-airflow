import logging
from sqlalchemy.sql import text
import pdb
import dc_file_fix
import ntpath

__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"


logger = logging.getLogger(__name__)






def import_animals(filename, filelist, db_engine):   
    fixed_filename = check_for_fixed_file(filename, filelist)
    if fixed_filename is not None:
        parse_animals(fixed_filename, db_engine)





def parse_animals(data_csv, db_engine):
    # create animal temp table
    create_table(db_engine, 'dairy_comp.temp_import_animals', 
        create_temp_import_animals_table_statement.format('dairy_comp.temp_import_animals'))

    # create animal staging table
    create_table(db_engine, 'dairy_comp.temp_staging_animals', 
        create_temp_staging_animals_table_statement.format('dairy_comp.temp_staging_animals')) 
    
    # create animal event staging table
    create_table(db_engine, 'dairy_comp.temp_staging_animal_events',
        create_temp_staging_animal_events_table_statement.format('dairy_comp.temp_staging_animal_events'))

    # load data from csv
    populate_table_from_csv('dairy_comp.temp_import_animals', data_csv, db_engine)

    # copy distinct animal events to staging table
    execute_statement(stage_animal_events_statement, db_engine)

    # # update and copy animals to dairy_comp.animal_events table
    execute_statement(transfer_animal_event_statement, db_engine)

    # copy distinct animals to staging table
    execute_statement(stage_distinct_animals_statement, db_engine)

    # update and copy animals to dairy_comp.animals table
    execute_statement(transfer_animals_statement, db_engine)

    # clean up temp tables
    drop_table('dairy_comp.temp_import_animals', db_engine)
    drop_table('dairy_comp.temp_staging_animals', db_engine)





def import_active_animals(filename, filelist, db_engine):   
    fixed_filename = check_for_fixed_file(filename, filelist)
    if fixed_filename is not None:
        parse_active_animals(fixed_filename, db_engine)





def parse_active_animals(data_csv, db_engine):
    # create temp table
    create_table(db_engine, 'dairy_comp.temp_import_active_animals', 
        create_temp_import_active_animals_table_statement.format('dairy_comp.temp_import_active_animals'))

    # create animal staging table
    create_table(db_engine, 'dairy_comp.temp_staging_active_animals', 
        create_temp_staging_active_animals_table_statement.format('dairy_comp.temp_staging_active_animals')) 
    
    # load data from csv
    populate_table_from_csv('dairy_comp.temp_import_active_animals', data_csv, db_engine)

    # copy distinct active animals to staging table
    execute_statement(stage_distinct_active_animals_statement, db_engine)

    # update and copy active animals to dairy_comp.animals table
    execute_statement(transfer_active_animals_statement, db_engine)

    # clean up temp tables
    drop_table('dairy_comp.temp_import_active_animals', db_engine)
    drop_table('dairy_comp.temp_staging_active_animals', db_engine)






def check_for_fixed_file(filename, filelist):
    # if this one isn't fixed
    if ntpath.basename(filename).split('.')[-1] == 'fixed':
        return filename
    else:
        # and there isn't an equivilant fixed file in the list
        if filename + ".fixed" not in filelist:
            # create a fixed file
            return dc_file_fix.fix_animal_file(filename)
        else:
            # it'll get to the fixed on on it's own
            return None




def create_table(db_engine, table_name, sql_statement):

    # check and delete if table already exists
    drop_table(table_name, db_engine)

    # create new temp table
    with db_engine.connect() as con:
        try:
            logger.info("Creating table " + table_name + " in " + db_engine.url.database + " database...")
            logger.debug('create_temp_table_statement = ' + str(sql_statement))
            con.execute(text(sql_statement))   
        except Exception as e:
            logger.error("Error creating the table " + table_name + " in " + db_engine.url.database + " database!")
            logger.error(e.args)
            exit(1)





def drop_table(table_name, db_engine):
    if db_engine.has_table(table_name.split('.')[1], schema=table_name.split('.')[0]):
        logger.debug("Deleting old (pre-existing) table: " + table_name + "...")
        statement = "drop table if exists {};"
        
        with db_engine.connect() as con:
            try:
                con.execute(statement.format(table_name))
            except Exception as e:
                logger.error("dc_event_import.drop_table(): Error deleting table " + table_name + " from database!")
                logger.error(e.args)
                exit(1)




def populate_table_from_csv(table_name, csv_location, db_engine):
    # 'copy_from' example from https://www.dataquest.io/blog/loading-data-into-postgres/
    # adapted to sqlalchemy using https://stackoverflow.com/questions/13125236/sqlalchemy-psycopg2-and-postgresql-copy
    with db_engine.connect() as con:
    
        # isolate a connection
        connection = db_engine.connect().connection

        # get the cursor
        cursor = connection.cursor()
    
        try:
            with open(csv_location, 'r') as f:
                next(f)  # Skip the header row.
                cursor.copy_from(f, table_name, sep=',', null='')    
            connection.commit()
            
        except Exception as e:
            logger.error("Error importing the table " + table_name + " in " + db_engine.url.database + " database from " + csv_location + "!")
            logger.error(e.args)
            exit(1)






def execute_statement(statement, db_engine):
    with db_engine.connect() as con:
        try:
            con.execute(text(statement))
        except Exception as e:
            logger.error("Error executing statement {}".format(statement))
            logger.error(e.args)
            exit(1)





# SQL STATEMENTS
create_temp_import_animals_table_statement = \
    'CREATE TABLE {} (' \
    ' id      INT,' \
    ' eid     VARCHAR(25),' \
    ' bdat    VARCHAR(25),' \
    ' cbrd    VARCHAR(12),' \
    ' cntl    INT,' \
    ' reg     VARCHAR(25),' \
    ' dbrd    VARCHAR(12),' \
    ' did     INT,' \
    ' dreg    VARCHAR(25),' \
    ' sid     VARCHAR(25),' \
    ' mgsir   VARCHAR(25),' \
    ' edat    VARCHAR(25),' \
    ' event   VARCHAR(25),' \
    ' dim     INT,' \
    ' date    VARCHAR(12),' \
    ' remark  VARCHAR(32),' \
    ' r       VARCHAR(12),' \
    ' t       INT,' \
    ' b       VARCHAR(12)' \
    ');'




# id, eid, bdat, cntl, edat, event, dim, date, remark, r, t, b
create_temp_staging_animal_events_table_statement = \
    'CREATE TABLE {} (' \
    ' id         INT, '\
    ' eid        VARCHAR(25),' \
    ' bdat       DATE, ' \
    ' cntl       INT,' \
    ' edat       DATE,' \
    ' event      VARCHAR(64),' \
    ' dim        INT,' \
    ' date       DATE,' \
    ' remark     VARCHAR(64),' \
    ' r          VARCHAR(10),' \
    ' t          INT,' \
    ' b          VARCHAR(10) '\
    ');' 




create_temp_staging_animals_table_statement = \
    'CREATE TABLE {} ( '\
    ' id      INT, '\
    ' eid     VARCHAR(25), '\
    ' bdat    DATE, '\
    ' cbrd    VARCHAR(12), '\
    ' cntl    INT, '\
    ' reg     VARCHAR(25), '\
    ' dbrd    VARCHAR(12), '\
    ' did     INT, '\
    ' dreg    VARCHAR(25), '\
    ' sid     VARCHAR(25), '\
    ' mgsir   VARCHAR(25) '\
    ');'




create_temp_import_active_animals_table_statement = \
    'CREATE TABLE {} ( '\
    ' id      INT, '\
    ' eid     VARCHAR(25), '\
    ' bdat    VARCHAR(25), '\
    ' cbrd    VARCHAR(12), '\
    ' cntl    INT, '\
    ' reg     VARCHAR(25), '\
    ' dbrd    VARCHAR(12), '\
    ' did     INT, '\
    ' dreg    VARCHAR(25), '\
    ' sid     VARCHAR(25), '\
    ' mgsir   VARCHAR(25), '\
    ' edat    VARCHAR(25) '\
    ');'



create_temp_staging_active_animals_table_statement = \
    'CREATE TABLE {} ( '\
    '  id      INT, '\
    '  eid     VARCHAR(25), '\
    '  bdat    DATE '\
    ' );'




    # Copy distinct animal events to staging 
stage_animal_events_statement = \
    "INSERT INTO dairy_comp.temp_staging_animal_events (id, eid, bdat, cntl, edat, event, dim, date, remark, r, t, b) "\
    "    SELECT id, trim(eid), TO_DATE(bdat,'MM/DD/YY'), MAX(cntl), TO_DATE(edat,'MM/DD/YY'), event, MAX(dim), MAX(TO_DATE(date,'MM/DD/YY')), MAX(remark), MAX(r), MAX(t), MAX(b) "\
    "    FROM dairy_comp.temp_import_animals "\
    "    GROUP BY id, eid, bdat, edat, event;"



# Copy animals events from staging if doesn't already exist in animals events table
transfer_animal_event_statement = \
    'INSERT INTO dairy_comp.animal_events (id, eid, bdat, cntl, edat, event, dim, date, remark, r, t, b) '\
    'SELECT id, eid, bdat, cntl, edat, event, dim, date, remark, r, t, b '\
    'FROM dairy_comp.temp_staging_animal_events t '\
    'WHERE NOT EXISTS (SELECT * FROM dairy_comp.animal_events WHERE id = t.id AND eid = t.eid AND bdat = t.bdat AND edat = t.edat AND event = t.event);'



       # id, eid, & bdat uniquely identifies animal
    # Unclear if any of these other fields are needed.
stage_distinct_animals_statement = \
    "INSERT INTO dairy_comp.temp_staging_animals (id, eid, bdat, cbrd, cntl, reg, dbrd, did, dreg, sid, mgsir) "\
    "    SELECT id, trim(eid), TO_DATE(bdat,'MM/DD/YY'), MAX(trim(cbrd)), MAX(cntl), MAX(trim(reg)), MAX(trim(dbrd)), "\
    "        MAX(did), MAX(trim(dreg)), MAX(trim(sid)), MAX(trim(mgsir)) "\
    "    FROM dairy_comp.temp_import_animals "\
    "    GROUP BY id, eid, bdat;"



    # Copy animals from staging if doesn't already exist in animals table
transfer_animals_statement = \
    'INSERT INTO dairy_comp.animals (id, eid, bdat, cbrd, cntl, reg, dbrd, did, dreg, sid, mgsir) '\
    'SELECT id, eid, bdat, cbrd, cntl, reg, dbrd, did, dreg, sid, mgsir '\
    'FROM dairy_comp.temp_staging_animals t '\
    'WHERE NOT EXISTS (SELECT * FROM dairy_comp.animals WHERE id = t.id AND eid = t.eid AND bdat = t.bdat);'



        # Copy distinct animals to staging
stage_distinct_active_animals_statement = \
    "INSERT INTO dairy_comp.temp_staging_active_animals (id, eid, bdat) "\
    "  SELECT distinct id, trim(eid), to_date(bdat,'MM/DD/YY') "\
    "    FROM dairy_comp.temp_import_active_animals "\
    "   WHERE trim(bdat) != '-'; "



   # If animal not in this new active animal file, then set active_flag and deactivation_date
transfer_active_animals_statement = \
    'UPDATE dairy_comp.animals a '\
    '   SET active_flag = FALSE, '\
    '       deactivation_date = current_date '\
    ' WHERE a.active_flag = TRUE '\
    '   AND NOT EXISTS (SELECT * FROM dairy_comp.temp_staging_active_animals t WHERE t.id = a.id and t.eid = a.eid and t.bdat = a.bdat); '
