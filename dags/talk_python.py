import re
from typing import Dict, List
import urllib

import arrow
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import parse_execution_date
from bs4 import BeautifulSoup
from lxml import etree
import pendulum
from pendulum.datetime import DateTime
import requests

import python_bytes


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "retries": 0,
}


def get_recent_episodes(
    recent_episodes_date: DateTime, exec_date: DateTime
) -> List[Dict]:
    number_re = re.compile("^\d+")

    tree = etree.parse(
        urllib.request.urlopen("https://talkpython.fm/episodes/rss_full_history")
    )
    root = tree.getroot()
    recent_episodes = []
    for item in root.iter("item"):
        tag_to_text = {}
        for child in item:
            # print("%s - %s" % (child.tag, child.text))
            tag_to_text[child.tag] = child.text
        id_ = tag_to_text["{http://www.itunes.com/dtds/podcast-1.0.dtd}episode"].strip()
        assert len(id_) > 0
        id_ = "TalkPythonToMe:" + id_

        title = tag_to_text["{http://www.itunes.com/dtds/podcast-1.0.dtd}title"].strip()
        assert len(title) > 0
        summary = tag_to_text["{http://www.itunes.com/dtds/podcast-1.0.dtd}summary"]
        if summary is None:
            summary = title
        else:
            summary = summary.strip()
        author = tag_to_text[
            "{http://www.itunes.com/dtds/podcast-1.0.dtd}author"
        ].strip()
        assert len(author) > 0
        episode_number = tag_to_text[
            "{http://www.itunes.com/dtds/podcast-1.0.dtd}episode"
        ].strip()
        episode_number = int(episode_number)
        d = tag_to_text["{http://www.itunes.com/dtds/podcast-1.0.dtd}duration"].strip()
        d = [int(x.strip()) for x in d.split(":")]
        if len(d) == 2:
            duration_in_seconds = d[0] * 60 + d[1]
        elif len(d) == 3:
            duration_in_seconds = d[0] * 3600 + d[1] * 60 + d[2]
        else:
            raise ValueError(d)
        keywords = tag_to_text[
            "{http://www.itunes.com/dtds/podcast-1.0.dtd}keywords"
        ].strip()
        keywords = [x.strip() for x in keywords.split(",")]
        raw_text = tag_to_text["description"].strip()
        assert len(raw_text) > 0
        ## possible but not necessary here:
        ## ensure that we separate some html elements as newlines
        # raw_text = raw_text.replace("</div>", "\n")
        # raw_text = raw_text.replace("</code>", "\n")
        # raw_text = raw_text.replace("</li>", "\n")
        # raw_text = raw_text.replace("</p>", "\n")
        parsed_text = BeautifulSoup(raw_text, "html.parser").get_text()
        url = tag_to_text["link"].strip()
        publication_date = tag_to_text["pubDate"]
        publication_date = arrow.get(publication_date, "D MMM YYYY").format(
            "YYYY-MM-DD"
        )

        if pendulum.parse(publication_date) < recent_episodes_date:
            # episode is too old and since episodes are sorted by publication date, we can stop here
            break

        # append transcript to both parsed and raw text
        repo_api_url = "https://api.github.com/repos/mikeckennedy/talk-python-transcripts/git/trees/master"
        transcript = python_bytes.fetch_transcript(
            episode_number, number_re, repo_api_url
        )
        # Skip episode for now as we only want episodes with transcripts
        if transcript is None:
            continue
        transcript = "\nEpisode transcript:\n" + transcript
        raw_text += transcript
        parsed_text += transcript

        doc_dict = {
            "id": id_,
            "version": "1",
            "source": "Talk Python To Me",
            "title": title,
            "document_type": "Podcast episode",
            "authors": [author],
            "publication_date": publication_date,
            "update_date": exec_date.to_date_string(),
            "urls": [url],
            "summary": summary,
            "raw_text": raw_text,
            "raw_text_format": "HTML",
            "parsed_text": parsed_text,
            "language": "English",
            "keywords": keywords,
            "extra": {
                "duration_in_seconds": duration_in_seconds,
                "episode_number": episode_number,
                "has_transcript": True,
            },
        }
        recent_episodes.append(doc_dict)
    return recent_episodes


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
        recent_episodes = get_recent_episodes(recent_episodes_date, exec_date)

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
