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




def import_events(filename, filelist, db_engine):   
    fixed_filename = check_for_fixed_file(filename, filelist)
    if fixed_filename is not None:
        parse_events(fixed_filename, db_engine)





def check_for_fixed_file(filename, filelist):
    # if this one isn't fixed
    if ntpath.basename(filename).split('.')[-1] == 'fixed':
        return filename
    else:
        # and there isn't an equivilant fixed file in the list
        if filename + ".fixed" not in filelist:
            # create a fixed file
            return dc_file_fix.fix_event_file(filename)
        else:
            # it'll get to the fixed on on it's own
            return None





def parse_events(event_csv, db_engine):
    # import data
    temp_event_table_name = 'dairy_comp.temp_import_events'  
    create_temp_event_table(temp_event_table_name, db_engine)
    populate_table_from_csv(temp_event_table_name, event_csv, db_engine)

    # stage and transfer data
    temp_staging_table_name = 'dairy_comp.temp_staging_events'
    create_temp_staging_event_table(temp_staging_table_name, db_engine)
    transfer_events_moving_window(temp_event_table_name, temp_staging_table_name, db_engine)
    perform_final_transfer(db_engine)

    # drop temp tables
    drop_table(temp_event_table_name, db_engine)
    drop_table(temp_staging_table_name, db_engine)
    




def create_temp_event_table(table_name, db_engine):
    # drop table if it exists
    drop_table(table_name, db_engine)

    # create the new table
    create_temp_table_statement = text( \
        'CREATE TABLE ' + table_name + ' ('\
        'id      INT,'\
        'bdat    VARCHAR(12),'\
        'lact    INT,'\
        'rc      INT,'\
        'pen     INT,'\
        'event   VARCHAR(32),'\
        'dim     INT,'\
        'date    VARCHAR(12),'\
        'remark  VARCHAR(32),'\
        'r       VARCHAR(12),'\
        't       INT,'\
        'b       VARCHAR(12)'\
        ');'
    )

    create_table(db_engine, table_name, create_temp_table_statement)






def create_temp_staging_event_table(table_name, db_engine):
    # drop table if it exists
    drop_table(table_name, db_engine)

    # create the new table
    create_temp_table_statement = text( \
        'CREATE TABLE ' + table_name + ' ('\
        'id      INT,'\
        'bdat    DATE,'\
        'lact    INT,'\
        'rc      INT,'\
        'pen     INT,'\
        'event   VARCHAR(32),'\
        'dim     INT,'\
        'date    DATE,'\
        'remark  VARCHAR(32),'\
        'r       VARCHAR(12),'\
        't       INT,'\
        'b       VARCHAR(12)'\
        ');'
    )

    create_table(db_engine, table_name, create_temp_table_statement)






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






def transfer_events_moving_window(from_table, to_table, db_engine):
    transfer_statement = "insert into " + to_table + " (id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b) "\
    "select distinct id, to_date(bdat,'MM/DD/YY'), lact, rc, pen, trim(event), dim, to_date(date,'MM/DD/YY'), trim(remark), trim(r), t, trim(b) "\
    "from " + from_table + " "\
    "where to_date(date,'MM/DD/YY') > ((select max(to_date(date,'MM/DD/YY')) from dairy_comp.temp_import_events) - integer '7');"

    with db_engine.connect() as con:
        try:
            con.execute(transfer_statement)
        except Exception as e:
            logger.error("Error inserting data from the " + from_table + " table "\
                         "to the " + to_table + " table in the " + db_engine.url.database + " database!")
            logger.error(e.args)
            exit(1)





def transfer_events(from_table, to_table, db_engine):
    transfer_statement = "insert into " + to_table + " (id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b) "\
    "select distinct id, to_date(bdat,'MM/DD/YY'), lact, rc, pen, trim(event), dim, to_date(date,'MM/DD/YY'), trim(remark), trim(r), t, trim(b) "\
    "from " + from_table + " "\
    "where to_date(date,'MM/DD/YY') > ((select max(to_date(date,'MM/DD/YY')) from dairy_comp.temp_import_events) - integer '7');"

    with db_engine.connect() as con:
        try:
            con.execute(transfer_statement)
        except Exception as e:
            logger.error("Error inserting data from the " + from_table + " table "\
                         "to the " + to_table + " table in the " + db_engine.url.database + " database!")
            logger.error(e.args)
            exit(1)





def perform_final_transfer(db_engine):

    # delete pre-existing recoreds from dairy_comp.events
    delete_statement = 'delete from dairy_comp.events where date in (select distinct date from dairy_comp.temp_staging_events);'

    with db_engine.connect() as con:
        try:
            logger.info("Deleting events from dairy_comp.events where date in dairy_comp.temp_staging_events...")
            logger.debug('delete_statement = ' + str(delete_statement))
            con.execute(delete_statement)   
        except Exception as e:
            logger.error("Error deleting overlapping dates from dairy_comp.events!")
            logger.error(e.args)
            exit(1)

    # final transfer from staging table to main event table
    insert_statement = 'insert into dairy_comp.events (id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b) '\
                       'select id, bdat, lact, rc, pen, event, dim, date, remark, r, t, b '\
                       'from dairy_comp.temp_staging_events;'

    with db_engine.connect() as con:
        try:
            logger.info("Transferring from dairy_comp.temp_staging_events to dairy_comp.events in " + db_engine.url.database + " database...")
            logger.debug('insert_statement = ' + str(insert_statement))
            con.execute(insert_statement)   
        except Exception as e:
            logger.error("Error transferring from dairy_comp.temp_staging_events to dairy_comp.events in " + db_engine.url.database + " database!")
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




def create_table(db_engine, table_name, sql_statement):

    # check and delete if table already exists
    drop_table(table_name, db_engine)

    # create new temp table
    with db_engine.connect() as con:
        try:
            logger.info("Creating table " + table_name + " in " + db_engine.url.database + " database...")
            logger.debug('create_temp_table_statement = ' + str(sql_statement))
            con.execute(sql_statement)   
        except Exception as e:
            logger.error("Error creating the table " + table_name + " in " + db_engine.url.database + " database!")
            logger.error(e.args)
            exit(1)









