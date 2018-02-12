from airflow.plugins_manager import AirflowPlugin
from AstroBigQueryPlugin.hooks.astro_big_query_hook import AstroBigQueryHook
from AstroBigQueryPlugin.operators.astro_big_query_operator import AstroBigQueryOperator


class AstroBigQueryPlugin(AirflowPlugin):
    name = "AstroBigQueryPlugin"
    operators = [AstroBigQueryOperator]
    # Leave in for explicitness
    hooks = [AstroBigQueryHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
