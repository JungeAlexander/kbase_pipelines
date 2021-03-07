from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import parse_execution_date
from bs4 import BeautifulSoup
import pendulum
import requests

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
    ### Talk Python podcast

    Find, filter and load recent Talk Python podcast episodes.
    To avoid heavy use of XCom between tasks, there are only two tasks:
    One generating JSON for new episodes to load; one adding episodes transcripts
    and loading the podcast episodes into kbase.
    """

    @task()
    def get_new_episodes(**kwargs) -> List[Dict]:
        """
        #### Find new episodes and convert them to JSON

        New episodes are recent episodes that have not been added to kbase yet.
        XML podcast feeds are converted to JSON
        """
        exec_date = parse_execution_date(kwargs["execution_date"])
        time_frame = pendulum.duration(days=int(Variable.get("podcast_recent_days")))

        recent_episodes_date = exec_date - time_frame
        api_connection_id = Variable.get("api_connection_id")
        connection = python_bytes.get_connection(api_connection_id)
        token_headers = python_bytes.get_request_headers(connection)
        recent_episodes = get_recent_episodes(recent_episodes_date)

        new_episodes = python_bytes.remove_existing_episodes(
            recent_episodes, connection, token_headers
        )
        return new_episodes

    @task()
    def load_episodes(new_episodes: List[Dict]):
        """
        #### Load episodes into kbase
        """
        api_connection_id = Variable.get("api_connection_id")
        connection = python_bytes.get_connection(api_connection_id)
        token_headers = python_bytes.get_request_headers(connection)
        for episode in new_episodes:
            id_ = episode["id"]
            r = requests.post(
                f"{connection.host}/documents/", json=episode, headers=token_headers
            )
            if r.status_code != 200:
                print(f"Failed: {id_}")
                continue

    new_episodes = get_new_episodes(execution_date="{{ execution_date }}")
    load_episodes(new_episodes)


d = talk_python_dag()
