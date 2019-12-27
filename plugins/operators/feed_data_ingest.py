"""
Wrapper bit to implement parsing and importing dry matter intake files from feed management
software into a database or file
"""
import sys
import os.path
import logging
import tarfile
import uuid
import tempfile
import shutil

from sqlalchemy import create_engine
from operators import feedwatch_parser
from operators import config

__author__ = "Oscar Zhang; Steven Wangen"
__version__ = "0.3"
__email__ = "tzhang383@wisc.edu; srwangen@wisc.edu"
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
        db_engine = create_engine(dialect + '://' + user + ':' + password + '@' + host + ':' + port + '/' + database,
                                  echo=db_log)

    except:
        logger.error("Can't connect to database: " + str(sys.exc_info()))
        sys.exit(1)

    return db_engine


def data_ingest(file_directory, is_testing, farm_id, db_log=True):
    # set farm id
    if farm_id is None:
        logging.error("Farm id not specified!")
        exit(1)

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

    # iterate over all the files
    for filename in filenames:
        logging.info("parsing files: " + str(filename))

        # make sure the file exists
        if os.path.exists(filename):

            # if it's compressed (tar), uncompress
            if filename.lower().endswith('.gz'):

                # make a temp directory
                tmp_path = tempfile.mkdtemp(suffix=str(uuid.uuid4()), prefix='feed_ingest_tmp_')

                # extract into temp dir
                tar = tarfile.open(filename, "r:gz")
                tar.extractall(path=tmp_path)

                # iterate through contents
                for subdir, dirs, files in os.walk(tmp_path):
                    for file in files:
                        # print os.path.join(subdir, file)
                        filepath = subdir + os.sep + file

                        if filepath.endswith(".csv"):
                            feedwatch_parser.parse_file(is_testing, farm_id, filepath, db_engine)

                # delete temp directory once complete
                logging.debug("finished parsing files from .gz - deleting temp directory...")
                shutil.rmtree(tmp_path)

            # if it's not a tar, just parse
            else:
                feedwatch_parser.parse_file(is_testing, farm_id, filename, db_engine)
        else:
            logging.error('File ' + filename + ' not found!')
