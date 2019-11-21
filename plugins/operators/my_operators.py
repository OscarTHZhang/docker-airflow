import logging
import datetime
import os

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from operators import feedwatch_parser

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
        log.info("Hello World!")
        log.info("Starting the StartOperator")
        task_instance = context['task_instance']
        sensor_minute = task_instance.xcom_pull('my_file_sensor_task', key='sensed_file_path')
        log.info('Valid minute as determined by sensor: {}'.format(sensor_minute))


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
        # files = context['task_instance'].xcom_pull()
        files = context['task_instance'].xcom_pull(key='file_list')
        # I want to pull this variable from the context

        # for file in os.listdir(self.directory):
        #     if file.endswith('.csv'):
        #         files.append(file)
        #
        log.info(files)
        for f in files:
            feedwatch_parser.parse_file(test=True, farm_id="123", filename=f, db_engine=None)

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
