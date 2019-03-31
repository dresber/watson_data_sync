"""
"""

# --------------------------------------- #
#               imports                   #
# --------------------------------------- #
import os
import json

from yaml import safe_load

from frame_handle import FrameHandle, TOKEN_POS

from utils.logging_handle import LoggingHandle

# --------------------------------------- #
#              definitions                #
# --------------------------------------- #
MODULE_LOGGER_HEAD = "watson_sync -> "

APP_VERSION = "v99-99-99"

# --------------------------------------- #
#              global vars                #
# --------------------------------------- #
logger = LoggingHandle()

# --------------------------------------- #
#              functions                  #
# --------------------------------------- #
def setup_logging(level):
    """ basic setup for the logging module
    """

    logger.set_logging_level(level)
    logger.set_cmd_line_logging_output()

    logger.add_global_except_hook()

    logger.add_file_logger("../logs/watson_sync.log")


# --------------------------------------- #
#               classes                   #
# --------------------------------------- #


# --------------------------------------- #
#                main                     #
# --------------------------------------- #
if __name__ == "__main__":

    with open("../config/sync_config.yml", "r") as config_f:
        sync_config = safe_load(config_f)

    setup_logging(sync_config["general"]["debug_level"])

    logger.info(MODULE_LOGGER_HEAD + "---------------- SCRIPT STARTED {} --------------".format(APP_VERSION))

    if sync_config["watson"]["frames_path"]:
        frames_path = sync_config["watson"]["frames_path"]
    else:
        frames_path = os.getenv("APPDATA") + "/watson/frames"

    with open(frames_path, "r") as frames_f:
        raw_frames = json.load(frames_f)

        frame_handle = FrameHandle(raw_frames,
                                   sync_config["watson"]["user_name"],
                                   sync_config["watson"]["user_team"],
                                   sync_config["elastic"])

        frames_to_pull = frame_handle.synchronize_frames()

    if frames_to_pull:
        with open(frames_path, "w") as frames_f:
            for idx, frame in enumerate(raw_frames):
                if frame[TOKEN_POS] in frames_to_pull:
                    raw_frames[idx] = frames_to_pull[frame[TOKEN_POS]]
                    del frames_to_pull[frame[TOKEN_POS]]

            for raw_frame in frames_to_pull:
                raw_frames.append(raw_frame)

            json.dump(raw_frames, frames_f, indent=1, ensure_ascii=False)

    logger.info(MODULE_LOGGER_HEAD + "---------------- SCRIPT FINISHED --------------")
