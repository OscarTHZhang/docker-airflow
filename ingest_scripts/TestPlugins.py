from airflow.plugins_manager import AirflowPlugin
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults


class TestOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(TestOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        pass


class TestPlugin(AirflowPlugin):
    name = "test_plugin"
    operators = [TestOperator]