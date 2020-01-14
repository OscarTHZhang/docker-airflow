#!/usr/bin/env python
""" Wrapper bit to implement parsing and importing dry matter intake 
    files from feed management software into a database or file """


import os.path
import logging

from IngestAPI.MilkingDataIngest import alpro_parser
from configs.getEngine import get_db_engine


__author__ = "Steven Wangen, Oscar Zhang"
__version__ = "1.0.2"
__email__ = "srwangen@wisc.edu, tzhang383@wisc.edu"
__status__ = "Development"


logger = logging.getLogger(__name__)


def data_ingest(file_directory, is_testing, farm_id, db_log=True):
    # set farm id
    if farm_id is None:
        logging.error("Farm id not specified!")
        exit(1)

    # create db engine, pass db_logging boolean
    db_engine = None
    if not is_testing:
        db_engine = get_db_engine(db_log)

    # determine files
    path = file_directory
    filenames = []
    if not os.path.isdir(path):
        filenames.append(path)
    elif os.path.isdir(path):
        for fn in os.path(path):
            filenames.append(fn)

    # parse files
    for filename in filenames:
        logging.info("parsing files: " + str(filename))
        if os.path.exists(filename):
            alpro_parser.parse_file(is_testing, farm_id, filename, db_engine)
        else:
            logging.error('File ' + filename + ' not found!')