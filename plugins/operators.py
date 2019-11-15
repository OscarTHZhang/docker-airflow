import logging
import datetime
import os

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from ingest_scripts.python.feed_ingest.data_ingest import feedwatch_parser

log = logging.getLogger(__name__)
os.chdir("/home/oscar/PycharmProjects/docker-airflow")


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
        log.info("Hello World!")
        log.info("Starting the StartOperator at {} on {}".format(datetime.time, datetime.date))
        task_instance = context['task_instance']
        sensor_minute = task_instance.xcom_pull('my_file_sensor_task', key='sensed_file_path')
        log.info('Valid minute as determined by sensor: {}'.format(sensor_minute))


class ScriptParser(BaseOperator):
    """
    This class includes the parsing script
    """

    @apply_defaults
    def __init__(self, directory, *args, **kwargs):
        super(ScriptParser, self).__init__(*args, **kwargs)
        self.directory = directory

    def execute(self, context):
        log.info("Initiate ScriptParser Operator at {} on {}".format(datetime.time, datetime.time))
        log.info("Parsing file in {}".format(self.directory))
        feedwatch_parser(test=True, farm_id="123", filename=self.directory, db_engine=None)
        task_instance = context['task_instance']
        task_instance.xcom_push('parsing_object', self.directory)


class MyPlugins(AirflowPlugin):
    """
    This class will initialize the plugins I made above
    """
    name = "my_first_plugin"
    operators = [StartOperator, ScriptParser]
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
