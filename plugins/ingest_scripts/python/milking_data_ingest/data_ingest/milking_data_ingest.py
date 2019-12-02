#!/usr/bin/env python
""" Wrapper bit to implement parsing and importing dry matter intake 
    files from feed management software into a database or file """


import sys
import os.path
import argparse
import logging
import pdb
from sqlalchemy import create_engine

import config
import alpro_parser


__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"



logger = logging.getLogger(__name__)



def get_db_engine(db_log):
    
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
    if 'db_database' in dir(config):
        database = config.db_database
    else:
        logger.error("Database not specified in config file!")

    try:
        db_engine = create_engine(dialect + '://' + user + ':' + password + '@' + host + ':' + port + '/' + database, echo=db_log)
        
    except:
        logger.error("Can't connect to database: " + str(sys.exc_info()))
        sys.exit(1)
    
    return db_engine




def parse_args():
    parser = argparse.ArgumentParser(description='Read data files from DeLaval AlPro software.')
    parser.add_argument('-t', '--TEST', action='store_true',
                    help='Run the program and output results to test_output.csv in the local folder. If '
                        + 'no input file is specified, will look to use a test_input.csv file located in '
                        + 'the local directory.')
    parser.add_argument('-d', '--db_log', action='store_true',
                    help='Turn on debug logging for the database communication (sqlalchemy).')
    parser.add_argument('-i', '--farm_id', nargs='?', const=None, default=None,
                    help='Unique id of the farm from which the milking data is derived')
    parser.add_argument("-v", "--verbose", const=1, default=0, type=int, nargs="?",
                    help="increase verbosity: 0 = only warnings, 1 = info, 2 = debug. No number means info. Default is no verbosity.")
    parser.add_argument('filenames', metavar='filenames', type=str, nargs='*',
                    help='name of file(s) to be processed')
    
    args = parser.parse_args()
    main(args)




def main(args):
    # set logging level
    if args.verbose == 0:
        log_level = logging.WARN
    elif args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose == 2:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(levelname)s %(message)s')

    logging.debug('args = ' + str(args))
    
    # set farm id
    if(args.farm_id == None):
        logging.error("Farm id not specified!")
        exit(1)

    # test mode
    if args.TEST:
        logging.info('Running in test mode - output local csv');
    if len(args.filenames) == 0:       
        logging.info('No filenames specified - using local test_input.csv')
        args.filenames = ["test_input.csv"] 

    # parse files 
    # create db engine, pass db_logging boolean
    db_engine = get_db_engine(args.db_log)
    for filename in args.filenames:
        logging.info("parsing files: " + str(filename))
        if os.path.exists(filename):
            alpro_parser.parse_file(args.TEST, args.farm_id, filename, db_engine)
        else:
            logging.error('File ' + filename + ' not found!')
            



if __name__ == "__main__":
    # main(sys.argv)
    # pdb.set_trace()
    parse_args()

