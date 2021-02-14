import base64
import time
from typing import Dict
import json

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import provide_session
import pendulum
from pendulum.datetime import DateTime
import requests


@provide_session
def get_request_headers(connection_id: str, session=None) -> Dict:
    connection_query = session.query(Connection).filter(
        Connection.conn_id == connection_id
    )
    connection: Connection = connection_query.one_or_none()
    login_data = {
        "username": connection.login,
        "password": connection.password,
    }
    r = requests.post(f"{connection.host}/token", data=login_data)
    tokens = r.json()
    a_token = tokens["access_token"]
    token_headers = {"Authorization": f"Bearer {a_token}"}
    return token_headers


def fetch_transcript(episode_number, number_re, sleep_seconds=3):
    time.sleep(sleep_seconds)
    r = requests.get(
        "https://api.github.com/repos/mikeckennedy/python_bytes_show_notes/git/trees/master"
    )
    j = r.json()
    tree_url = None
    for e in j["tree"]:
        if e["path"] == "transcripts":
            tree_url = e["url"]
    assert tree_url is not None

    r = requests.get(tree_url)
    j = r.json()

    blob_url = None
    for e in j["tree"]:
        m = number_re.match(e["path"])
        if m and m.group(0) == f"{episode_number:03}":
            blob_url = e["url"]
    assert blob_url is not None

    r = requests.get(blob_url)
    j = r.json()
    content = base64.b64decode(j["content"]).decode("utf-8")
    return content


def get_recent_episodes(recent_episodes_date: DateTime) -> Dict:
    token_headers = get_request_headers("dbapi")
    return {}


def remove_existing_episodes(recent_episodes: Dict) -> Dict:
    token_headers = get_request_headers("dbapi")
    return {}


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
def python_bytes_dag():
    """
    ### Python Bytes podcast

    Find, filter and load recent Python Bytes podcast episodes.
    To avoid heavy use of XCom between tasks, there are only two tasks:
    One generating JSON for new episodes to load; one adding episodes transcripts
    and loading the podcast episodes into kbase!
    """

    @task()
    def get_new_episodes(**kwargs) -> Dict:
        """
        #### Find new episodes and convert them to JSON

        New episodes are recent episodes that have not been added to kbase yet.
        XML podcast feeds are converted to JSON
        """
        exec_date = parse_execution_date(kwargs["execution_date"])
        time_frame = pendulum.duration(days=int(Variable.get("podcast_recent_days")))
        recent_episodes_date = exec_date - time_frame
        recent_episodes = get_recent_episodes(recent_episodes_date)
        new_episodes = remove_existing_episodes(recent_episodes)
        return new_episodes

    @task()
    def load_episodes(new_episodes: Dict):
        """
        #### Load episodes into kbase
        """
        print(new_episodes)

    new_episodes = get_new_episodes(execution_date="{{ execution_date }}")
    load_episodes(new_episodes)


d = python_bytes_dag()
