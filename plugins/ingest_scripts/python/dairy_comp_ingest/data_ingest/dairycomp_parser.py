#!/usr/bin/env python
""" Parsing and importing event files from VAS dairycomp software
    into a database or csv """



import sys
import datetime
import logging
import pdb
import time
import argparse
import os.path
import ntpath
from sqlalchemy import create_engine

import config
import dc_event_import
import dc_animal_import



__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"



logger = logging.getLogger(__name__)


def get_db_engine(db_log, db_name):
    
    # user
    if 'db_user' in dir(config):
        user = config.db_user
    else:
        logger.error("User not specified in config file!")
    
    # dialect
    if 'db_dialect' in dir(config):
        dialect = config.db_dialect
    else:
        logger.error("Dialect not specified in config file!")
    
    # host
    if 'db_host' in dir(config):
        host = config.db_host
    else:
        logger.error("Host not specified in config file!")

    # password
    if 'db_password' in dir(config):
        password = config.db_password
    else:
        logger.error("Password not specified in config file!")

    # port
    if 'db_port' in dir(config):
        port = config.db_port
    else:
        logger.error("Port not specified in config file!")

    # database
    if db_name is None:
        if 'db_database' in dir(config):
            database = config.db_database
        else:
            logger.error("Database not specified in config file!")
    else:
        database = db_name

    try:
        db_engine = create_engine(dialect + '://' + user + ':' + password + '@' + host + ':' + port + '/' + database, echo=db_log)
        
    except:
        logger.error("Can't connect to database: " + str(sys.exc_info()))
        sys.exit(1)
    
    return db_engine





def parse_args():
    parser = argparse.ArgumentParser(description="Read data files from dairycomp extract. Example usage pattern: '...(TBD)' ")
    
    parser.add_argument('filename', metavar='filename', type=str, nargs=1,
                    help='name of file (or directory) to be processed')
    parser.add_argument('-t', '--TEST', action='store_true',
                    help='Run the program and output results to test_output.csv in the local folder. If '
                        + 'no input file is specified, will look to use a test_input.csv file located in '
                        + 'the local directory.')
    parser.add_argument('-d', '--db_log', action='store_true',
                    help='Turn on debug logging for the database communication (sqlalchemy).')
    parser.add_argument('-i', '--farm_id', nargs='?', const=None, default=None,
                    help='Unique id of the farm from which the feed data is derived')
    parser.add_argument("-v", "--verbose", const=1, default=0, type=int, nargs="?",
                    help="increase verbosity: 0 = only warnings, 1 = info, 2 = debug. No number means info. Default is no verbosity.")
    parser.add_argument("-c", "--database-connection", type=str, nargs=1,
                    help="specify the name of the database to be written to (overrides the config.py file)")

    
    args = parser.parse_args()
    main(args)





def populate_file_list(directory, input_files):
    # swiped from https://stackoverflow.com/questions/9816816/get-absolute-paths-of-all-files-in-a-directory
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            input_files.append(os.path.abspath(os.path.join(dirpath, f)))    





def parse_file(farm_id, filename, filelist, db_engine):

    
    
    # determine type of file - need to worry about the other event types?
    
    # import animals
    if(ntpath.basename(filename)[5] == '1'):
        dc_animal_import.import_animals(filename, filelist, db_engine)
    
    # import active animals
    elif(ntpath.basename(filename)[5] == '2'):
        dc_animal_import.import_active_animals(filename, filelist, db_engine)
    
    # import events
    elif(ntpath.basename(filename)[5] == '5'):
        dc_event_import.import_events(filename, filelist, db_engine)

    # import breeding events
    elif(ntpath.basename(filename)[5] == '6'): # breeding event
        dc_event_import.import_events(filename, filelist, db_engine)

    else:
        logging.warn(("dairycomp_parser - file {} not recognized as valid dairycomp data file - skipping...").format(filename))





def main(args):
    # set logging level
    if args.verbose == 0:
        log_level = logging.WARN
    elif args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose == 2:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s %(message)s')

    logging.debug('args = ' + str(args))
    
    # set farm id
    if(args.farm_id == None):
        logging.error("Farm id not specified!")
        exit(1)

    # test mode
    if args.TEST:
        logging.info('Running in test mode - output local csv');
    # if len(args.filenames) == 0:       
    #     logging.info('No filenames specified - using local test_input.csv')
    #     args.filenames = ["test_input.csv"] 

    
    # create db engine, pass db_logging boolean
    db_engine = None
    if not args.TEST:
        if args.database_connection is not None:
            database_name = args.database_connection[0]
            logger.info("overwriting config.py database specification with: %s", database_name)
            db_engine = get_db_engine(args.db_log, database_name)
        else:
            db_engine = get_db_engine(args.db_log, None)

    # determine files 
    path = args.filename[0]
    filenames = []
    populate_file_list(path, filenames)

    results = []

    logging.info("parsing files: " + str(filenames))
    
    for fn in filenames:    
        # make sure the file exists
        if os.path.exists(fn):
            logging.info("parsing file " + fn)
            parse_file(args.farm_id, fn, filenames, db_engine)
                # if result[0] == 'component':
                #     component_results.append(result[1])
        else:
            logging.error('File ' + fn + ' not found!')
            exit(1)

    logging.warn('Parsing complete - processed the following files: ' + str(filenames))





if __name__ == "__main__":
    parse_args()

