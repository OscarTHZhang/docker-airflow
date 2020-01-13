""" 
Parsing and importing event files from VAS dairycomp software
into a database or csv 
"""

import logging
import os.path
import ntpath

from IngestAPI.DairycompDataIngest import dc_event_import, dc_animal_import
from configs.getEngine import get_db_engine

__author__ = "Steven Wangen, Oscar Zhang"
__version__ = "1.0.2"
__email__ = "srwangen@wisc.edu, tzhang383@wisc.edu"
__status__ = "Development"

logger = logging.getLogger(__name__)


def populate_file_list(directory, input_files):
    # swiped from https://stackoverflow.com/questions/9816816/get-absolute-paths-of-all-files-in-a-directory
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            input_files.append(os.path.abspath(os.path.join(dirpath, f)))


def parse_file(filename, filelist, db_engine):
    # determine type of file - need to worry about the other event types?

    # import animals
    if ntpath.basename(filename)[5] == '1':
        dc_animal_import.import_animals(filename, filelist, db_engine)

    # import active animals
    elif ntpath.basename(filename)[5] == '2':
        dc_animal_import.import_active_animals(filename, filelist, db_engine)

    # import events
    elif ntpath.basename(filename)[5] == '5':
        dc_event_import.import_events(filename, filelist, db_engine)

    # import breeding events
    elif ntpath.basename(filename)[5] == '6':  # breeding event
        dc_event_import.import_events(filename, filelist, db_engine)

    else:
        logging.warn("dairycomp_parser - file {} not recognized as valid dairycomp data file - skipping...".format(filename))


def data_ingest(file_directory, is_testing, farm_id, db_log=True):
    # set farm id
    if farm_id is None:
        logging.error("Farm id not specified!")
        exit(1)

    # test mode
    if is_testing:
        logging.info('Running in test mode - output local csv')
    # if len(args.filenames) == 0:       
    #     logging.info('No filenames specified - using local test_input.csv')
    #     args.filenames = ["test_input.csv"] 

    # create db engine, pass db_logging boolean
    db_engine = None
    if not is_testing:
        db_engine = get_db_engine(db_log)

    # determine files 
    path = file_directory
    filenames = []
    populate_file_list(path, filenames)

    logging.info("parsing files: " + str(filenames))

    for fn in filenames:
        # make sure the file exists
        if os.path.exists(fn):
            logging.info("parsing file " + fn)
            parse_file(fn, filenames, db_engine)
            # if result[0] == 'component':
            #     component_results.append(result[1])
        else:
            logging.error('File ' + fn + ' not found!')
            exit(1)

    logging.warn('Parsing complete - processed the following files: ' + str(filenames))