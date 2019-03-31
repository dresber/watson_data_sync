"""
"""
# -*- coding: UTF-8 -*-


# --------------------------------------- #
#               imports                   #
# --------------------------------------- #
import logging
import os
import sys

from logging.handlers import TimedRotatingFileHandler

# --------------------------------------- #
#              definitions                #
# --------------------------------------- #
STRING_TO_LOGGING_LEVEL = {"debug": logging.DEBUG,
                           "info":  logging.INFO,
                           "error": logging.ERROR}

DEFAULT_LOG_DESTINATION = ""

# --------------------------------------- #
#              global vars                #
# --------------------------------------- #
formatter = logging.Formatter("%(asctime)s: %(levelname)s \t %(message)s")


# --------------------------------------- #
#              functions                  #
# --------------------------------------- #


# --------------------------------------- #
#               classes                   #
# --------------------------------------- #
class LoggingHandle(object):
    __instance = None

    def __new__(cls):
        if LoggingHandle.__instance is None:
            LoggingHandle.__instance = object.__new__(cls)
        return LoggingHandle.__instance

    def __init__(self):
        self.logger_obj = logging.getLogger("app")
        self._error_callback = None
        self._info_callback = None

    def debug(self, msg):
        self.logger_obj.debug(msg)

    def error(self, msg):
        self.logger_obj.error(msg)
        if self._error_callback:
            self._error_callback(msg)

    def info(self, msg):
        self.logger_obj.info(msg)
        if self._info_callback:
            self._info_callback(msg)

    def warning(self, msg):
        self.logger_obj.warning(msg)

    def add_file_logger(self, folder_destination_of_log_files):
        """ will add an handler for writing log files to the desired folder

        :param folder_destination_of_log_files:
        :return: True if everything worked else False
        """
        file_handler = TimedRotatingFileHandler(folder_destination_of_log_files, when="midnight", interval=7)
        file_handler.setFormatter(formatter)
        self.logger_obj.addHandler(file_handler)

    def add_global_except_hook(self):
        """ will set an excepthook which will through an logger exception
        :return:
        """
        sys.excepthook = self._exception_handler

    def _check_log_directory(self, export_directory):
        """ will check if the log folder for the log files exists or creates it
        :return path to the log directory
        """
        if not os.path.exists(export_directory):
            try:
                os.makedirs(export_directory)
            except Exception as e:
                export_directory = DEFAULT_LOG_DESTINATION
                self.logger_obj.exception("could not create export directory {}\n{}".format(export_directory, e))

        return export_directory

    def _exception_handler(self, type, value, tb):
        """ general except hook to catch exception while program execution
        :param type: Exception type
        :param value: Exception message
        :param tb: Exception storage address
        """
        self.logger_obj.exception("Uncaught exception: {} {}".format(type, value))

    def set_logging_level(self, level):
        """ will set the global logging level
        :param level: string with level info (debug, info, error)
        """
        self.logger_obj.setLevel(STRING_TO_LOGGING_LEVEL[level])

    def set_cmd_line_logging_output(self):
        """ will add the logging outputs to command line
        :return:
        """
        cmd_handler = logging.StreamHandler(sys.stdout)
        cmd_handler.setFormatter(formatter)

        self.logger_obj.addHandler(cmd_handler)

    def set_callback_functions(self, info_callback, error_callback):
        self._error_callback = error_callback
        self._info_callback = info_callback


# --------------------------------------- #
#                main                     #
# --------------------------------------- #
