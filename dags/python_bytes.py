from typing import Dict
import json

from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval=Variable.get("update_schedule"),
    start_date=pendulum.parse(Variable.get("podcast_start_date")),
    catchup=True,
)
def python_bytes_dag():
    """
    ### Python Bytes podcast

    Find, filter and load recent Python Bytes podcast episodes.
    To avoid heavy use of XCom between tasks, there are only two tasks:
    One generating JSON for new episodes to load; one adding episodes transcripts
    and loading the podcast episodes into kbase.
    """

    @task()
    def get_new_episodes() -> Dict:
        """
        #### Find new episodes and convert them to JSON

        New episodes are recent episodes that have not been added to kbase yet.
        XML podcast feeds are converted to JSON
        """
        # TODO create airflow (and other users) in DB via script/notebook

        pass

    @task()
    def add_transcripts_and_load_episodes(new_episodes: Dict):
        """
        #### Add episode transcript and load episodes into kbase
        """
        pass

    new_episodes = get_new_episodes()
    add_transcripts_and_load_episodes(new_episodes)


d = python_bytes_dag()
