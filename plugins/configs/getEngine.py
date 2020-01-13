import logging
import sys

from sqlalchemy import create_engine
from configs import config

logger = logging.getLogger(__name__)


def get_db_engine(db_log):
    """
    This is a function that will get the database engine from the config.py
    :param db_log: boolean flag for whether log the db
    :return: database engine
    """
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