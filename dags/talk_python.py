from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum

import python_bytes


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "retries": 0,
}


@dag(
    default_args=default_args,
    schedule_interval=Variable.get("update_schedule"),
    start_date=pendulum.parse(Variable.get("podcast_start_date")),
    catchup=True,
)
def talk_python_dag():
    """
    ### TODO
    """

    @task()
    def get_new_episodes(**kwargs) -> List[Dict]:
        """
        #### TODO
        """
        api_connection_id = Variable.get("api_connection_id")
        connection = python_bytes.get_connection(api_connection_id)
        return [{}]

    @task()
    def load_episodes(new_episodes: List[Dict]):
        """
        #### TODO
        """
        pass

    new_episodes = get_new_episodes(execution_date="{{ execution_date }}")
    load_episodes(new_episodes)


d = talk_python_dag()
