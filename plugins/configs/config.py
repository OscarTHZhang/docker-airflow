"""
This is the configuration of Oscar's local database using Mac OSX 10.12.6

__author__ = "Oscar Zhang"
__email__ = "tzhang383@wisc.edu"
__version__ = '0.1'
__status__ = 'Development'
"""

db_password = "1234567890"
db_user = "dairybrain"
db_host = "host.docker.internal"  # host name only works on Mac OSX
db_port = '5432'
db_database = 'farms'
db_dialect = 'postgresql'