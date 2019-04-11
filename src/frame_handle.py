"""
"""

# --------------------------------------- #
#               imports                   #
# --------------------------------------- #
from datetime import datetime, timezone
from enum import Enum

from es_handle import ESHandle

from utils.logging_handle import LoggingHandle

# --------------------------------------- #
#              definitions                #
# --------------------------------------- #
START_TIME_POS = 0
STOP_TIME_POS = 1
PROJECT_NAME_POS = 2
TOKEN_POS = 3
TAGS_POS = 4
CHANGE_TIME_POS = 5

MODULE_LOGGER_HEAD = "frame_handle -> "
ES_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class SyncState(Enum):
    EXCEPTION = -1
    SYNCHRONIZED = 0
    PULL_FRAME = 1
    PUSH_FRAME = 2
    UPDATE_FRAME = 3


# --------------------------------------- #
#              global vars                #
# --------------------------------------- #
logger = LoggingHandle()


# --------------------------------------- #
#              functions                  #
# --------------------------------------- #
def _create_token_query(token, user):
    return {"query": {
            "bool": {
                "must": [{"match": {"token": token}},
                         {"match": {"user": user}}]
            }}}


def _create_user_query(user):
    return {"query": {
            "bool": {
                "must": [{"match": {"user": user}}]
            }}}


def _create_es_doc(frame, user, team):
    es_doc = frame.create_es_doc()
    es_doc["user"] = user
    es_doc["team"] = team
    logger.debug(MODULE_LOGGER_HEAD + "create doc frame: {}".format(es_doc))
    return es_doc


def _create_timestamp_from_date(date):
    return int(_utc_to_local(datetime.strptime(date, ES_DATE_FORMAT)).timestamp())


def _utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def _convert_es_doc_to_raw_frame(es_data):
    raw_frame = [None] * 6

    raw_frame[START_TIME_POS] = _create_timestamp_from_date(es_data["start_time"])
    raw_frame[STOP_TIME_POS] = _create_timestamp_from_date(es_data["stop_time"])
    raw_frame[CHANGE_TIME_POS] = _create_timestamp_from_date(es_data["storage_time"])
    raw_frame[PROJECT_NAME_POS] = es_data["project"]
    raw_frame[TAGS_POS] = es_data["tags"]
    raw_frame[TOKEN_POS] = es_data["token"]

    return raw_frame


# --------------------------------------- #
#               classes                   #
# --------------------------------------- #
class WatsonFrame(object):
    """ class to handle each frame as an object an create the ES doc directly
    """
    def __init__(self):
        self.start_time = 0
        self.stop_time = 0
        self.change_time = 0
        self.project = ""
        self.token = ""
        self.tags = []

    def create_frame_from_watson(self, raw_frame):
        self.start_time = raw_frame[START_TIME_POS]
        self.stop_time = raw_frame[STOP_TIME_POS]
        self.change_time = raw_frame[CHANGE_TIME_POS]
        self.project = raw_frame[PROJECT_NAME_POS]
        self.token = raw_frame[TOKEN_POS]
        self.tags = raw_frame[TAGS_POS]

    def create_es_doc(self):
        return {"start_time": self._create_utc_from_timestamp(self.start_time),
                "stop_time": self._create_utc_from_timestamp(self.stop_time),
                "token": self.token,
                "duration": self.duration()/3600,
                "tags": self.tags,
                "storage_time": self._create_utc_from_timestamp(self.change_time),
                "project": self.project}

    def create_raw_frame(self):
        raw_frame = [0] * 6
        raw_frame[START_TIME_POS] = self.start_time
        raw_frame[STOP_TIME_POS] = self.stop_time
        raw_frame[CHANGE_TIME_POS] = self.change_time
        raw_frame[PROJECT_NAME_POS] = self.project
        raw_frame[TOKEN_POS] = self.token
        raw_frame[TAGS_POS] = self.tags
        return raw_frame

    @staticmethod
    def _create_utc_from_timestamp(timestamp):
        return datetime.utcfromtimestamp(timestamp)

    def duration(self):
        return self.stop_time - self.start_time

    def create_frame_from_es(self, es_data):
        self.start_time = _create_timestamp_from_date(es_data["start_time"])
        self.stop_time = _create_timestamp_from_date(es_data["stop_time"])
        self.change_time = _create_timestamp_from_date(es_data["storage_time"])
        self.project = es_data["project"]
        self.token = es_data["token"]
        self.tags = es_data["tags"]


class FrameHandle(object):
    """ main class to synchronize the frames with ES
    """
    def __init__(self, raw_frames, user_name, user_team, es_config):
        self._create_frame_objects(raw_frames)
        self.user_name = user_name
        self.user_team = user_team
        self.es = ESHandle(es_config["server"], es_config["port"])
        self.es_index_name = es_config["index_name"]
        self.es_doc_type = es_config["doc_type"]

    def _create_frame_objects(self, raw_frames):
        self.frames = {}

        for frame in raw_frames:
            self.frames[frame[TOKEN_POS]] = WatsonFrame()
            self.frames[frame[TOKEN_POS]].create_frame_from_watson(frame)

    def synchronize_frames(self):
        """ does an automatic push of frames which are not on ES or older than locally, if there are duplicated tokens
        with the same storage time one will be deleted on ES. frames which should be updated locally are returned in
        a dictionary TOKEN: raw_frame
        :return: dictionary with raw_frames
        """
        self.es.create_index(self.es_index_name)

        frames_to_pull = {}

        for token, frame in self.frames.items():
            sync_result, data = self._check_token_exists_and_not_newer(token, frame)
            if sync_result == SyncState.PUSH_FRAME:
                self.es.upload_doc(self.es_index_name, self.es_doc_type, _create_es_doc(frame,
                                                                                        self.user_name,
                                                                                        self.user_team))
            elif sync_result == SyncState.PULL_FRAME:
                frames_to_pull[token] = _convert_es_doc_to_raw_frame(data)
            elif sync_result == SyncState.UPDATE_FRAME:
                self.es.update_doc(self.es_index_name, self.es_doc_type, id=data["_id"], doc_dict=_create_es_doc(frame,
                                                                                                        self.user_name,
                                                                                                        self.user_team))

        for es_frame in self._get_all_user_frames():
            tmp_frame = WatsonFrame()
            try:
                tmp_frame.create_frame_from_es(es_frame["_source"])
                if tmp_frame.token not in self.frames:
                    frames_to_pull[tmp_frame.token] = tmp_frame
            except Exception as e:
                logger.error(MODULE_LOGGER_HEAD + "could not convert token {} due to exception: {}".format(es_frame["_source"], e))

        return frames_to_pull

    def _check_token_exists_and_not_newer(self, token, frame):
        res = self.es.get_doc(index_name=self.es_index_name, doc_type=self.es_doc_type,
                              query=_create_token_query(token, self.user_name))
        if res["hits"]["total"] > 0:
            if res["hits"]["total"] > 1:
                logger.error(MODULE_LOGGER_HEAD + "duplicate token detection: {}".format(token))
                if res["hits"]["hits"][0]["_source"]["storage_time"] == res["hits"]["hits"][1]["_source"]["storage_time"]:
                    self.es.delete_entry(self.es_index_name, self.es_doc_type, res["hits"]["hits"][0]["_id"])
                    logger.info(MODULE_LOGGER_HEAD + "removed one of the duplicated token,"
                                                     " as the storage time was the same!")
            else:
                es_data_storage_time = _create_timestamp_from_date(res["hits"]["hits"][0]["_source"]["storage_time"])
                if frame.change_time == es_data_storage_time:
                    logger.debug(MODULE_LOGGER_HEAD + "frame {} already synchronized!".format(token))
                    return SyncState.SYNCHRONIZED, None
                elif frame.change_time > es_data_storage_time:
                    logger.debug(MODULE_LOGGER_HEAD + "local frame {} newer then on ES! Push local frame".format(token))
                    return SyncState.UPDATE_FRAME, res["hits"]["hits"][0]
                else:
                    logger.debug(MODULE_LOGGER_HEAD + "online frame {} newer than local! Pull  online frame".format(token))
                    return SyncState.PULL_FRAME, res["hits"]["hits"][0]["_source"]
        else:
            return SyncState.PUSH_FRAME, None

    def _get_all_user_frames(self):
        result = self.es.get_doc(self.es_index_name, self.es_doc_type, _create_user_query(self.user_name))
        if result["hits"]["total"] > 0:
            return result["hits"]["hits"]
        return []

# --------------------------------------- #
#                main                     #
# --------------------------------------- #
if __name__ == "__main__":
    pass

