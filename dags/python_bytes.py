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
    """

    @task()
    def get_recent_episodes():
        """
        #### Find recent episodes
        """
        pass

    @task()
    def remove_existing_episodes(recent_episodes):
        """
        #### Remove recent episodes which have already been added to kbase
        """
        # TODO create airflow (and other users) in DB via script/notebook
        pass

    @task()
    def add_transcripts_convert_to_json(new_episodes):
        """
        #### Add episode transcript and convert XML to JSON
        """
        pass

    @task()
    def load_episodes(episodes_to_load):
        """
        #### Load episodes into kbase
        """
        pass

    recent_episodes = get_recent_episodes()
    new_episodes = remove_existing_episodes(recent_episodes)
    new_episodes_with_transcripts = add_transcripts_convert_to_json(new_episodes)
    load_episodes(new_episodes_with_transcripts)


d = python_bytes_dag()
