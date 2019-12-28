"""
This file consists of a class of self-defined Airflow operators, and they will be used in all the data
pipelines of dairy data

__author__ = "Oscar Zhang"
__email__ = "tzhang383@wisc.edu"
__version__ = '0.1'
__status__ = 'Development'
"""

import datetime
import logging
import os

import psycopg2
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from operators import config
from operators import  feed_data_ingest


log = logging.getLogger(__name__)


class StartOperator(BaseOperator):
    """
    StartOperator is the starting of this sensing dag.
    It will print out some basic information for testing purposes
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(StartOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        """
        Start the dag; print out some basic information
        :param context:
        :return:
        """
        records = []
        try:
            connection = psycopg2.connect(
                dbname=config.db_database,
                user=config.db_user,
                password=config.db_password,
                host=config.db_host,
                port=config.db_port,
            )
            cursor = connection.cursor()
            query = "SELECT * FROM farm_datasource;"
            cursor.execute(query)
            records = cursor.fetchall()
            # example: [ (2, 1, 1, None, '/mnt/nfs/dairy/larson/dairycomp/', 'event', 'event_data_ingest.py', None,
            # True), (4, 2, 1, '', '/mnt/nfs/dairy/wangen/dairycomp/', 'event', 'event_data_ingest.py', '', False),
            # (6, 3, None, None, '/mnt/nfs/dairy/mystic_valley/tmrtracker/', 'feed', 'feed_data_ingest.py', '',
            # True), (5, 3, None, None, '/mnt/nfs/dairy/mystic_valley/dairycomp/', 'event', 'event_data_ingest.py',
            # '', True), (3, 1, 1, None,
            # '/Users/stevewangen/projects/cows_20_20/data_ingest/file_monitor/test/dairycomp', 'feed',
            # 'feed_data_ingest.py', None, True), (7, 6, None, None, '/mnt/nfs/dairy/arlington/agsource', 'agsource',
            #  'agsource_data_ingest.py', None, True)]
            records = StartOperator.normalize(records)
            connection.close()  # close the connection
        except (Exception, psycopg2.Error) as err:
            log.error(err)
            raise Exception('There is error in the connection/query with database')
        finally:
            log.info("Hello World!")
            log.info("Starting the StartOperator <{}>".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            task_instance = context['task_instance']
            task_instance.xcom_push('records', records)  # put records list into the task instance to be fetched

    @staticmethod
    def normalize(records):
        """
        Normalize the records into a list of dictionaries
            (<id>, <farm_id>, <software_id>, <software_version>, <file_location>, <datasource_type>, <script_name>,
            <script_arguments>, <active>)
        :param records: the records fetched from database
        :return: a list of dictionaries containing different entries querid from database
        """
        normalized_records = []
        for t in records:
            temp = dict()
            temp['id'] = t[0]
            temp['farm_id'] = t[1]
            temp['software_id'] = t[2]
            temp['software_version'] = t[3]
            temp['file_location'] = t[4]
            temp['datasource_type'] = t[5]
            temp['script_name'] = t[6]
            temp['script_arguments'] = t[7]
            temp['active'] = t[8]
            normalized_records.append(temp)
        return normalized_records


class DirectorySensor(BaseOperator):
    """
    Self-defined class for Directory Sensor
    """
    @apply_defaults
    def __init__(self, directory, *args, **kwargs):
        super(DirectorySensor, self).__init__(*args, **kwargs)
        self.directory = directory

    def execute(self, context):
        files = []
        log.info("Initiate DirecotrySensor Operator")
        log.info("Parsing file in {}".format(self.directory))
        for file in os.listdir(self.directory):
            if file.endswith(".csv"):
                files.append(os.path.join(self.directory, file))

        task_instance = context['task_instance']
        task_instance.xcom_push('file_list', files)
        return files


class ScriptParser(BaseOperator):
    """
    This class includes the parsing script
    """

    @apply_defaults
    def __init__(self, directory, *args, **kwargs):
        super(ScriptParser, self).__init__(*args, **kwargs)
        self.directory = directory

    def execute(self, context):
        log.info("Initiate ScriptParser Operator")
        log.info("Parsing file in {}".format(self.directory))
        script_map = context['task_instance'].xcom_pull(key='records')  # pull out script mapping
        log.info(str(script_map))
        script = ''
        farm_id = -1
        log.info(str(script_map))
        for mapping in script_map:
            path = mapping['file_location']
            log.info(path)
            log.info(os.path.join(self.directory.split('/')[-3:-1][0], self.directory.split('/')[-3:-1][1]))
            if path == os.path.join(self.directory.split('/')[-3:-1][0], self.directory.split('/')[-3:-1][1]):
                logging.info(str(mapping))
                farm_id = mapping['farm_id']
                script = mapping['script_name']
        files = context['task_instance'].xcom_pull(key='file_list')
        log.info(files)
        if script == '':
            raise FileNotFoundError('No valid script found!')
        if farm_id == -1:
            raise ValueError('No valid Farm ID!')

        # apply parsing script
        if script == 'feed_data_ingest.py':
            for f in files:
                feed_data_ingest.data_ingest(file_directory=f, is_testing=False, farm_id=farm_id, db_log=True)

        task_instance = context['task_instance']
        task_instance.xcom_push('parsing_object', self.directory)


class MyPlugins(AirflowPlugin):
    """
    This class will initialize the plugins I made above
    """
    name = "my_first_plugin"
    operators = [StartOperator, DirectorySensor, ScriptParser]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
