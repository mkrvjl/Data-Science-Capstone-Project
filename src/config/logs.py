import os
import sys
import logging

from logging.handlers import RotatingFileHandler

MAX_FILE_SIZE = 1024 * 1024 * 5  # 1024 BYTES * 1024 KILOBYTES * 5 = 5MB
LOG_PATH = os.path.abspath(".") + "/out/logs"
LOG_FORMAT = "%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s"
FORMATTER = logging.Formatter(LOG_FORMAT)


def get_console_handler() -> logging.StreamHandler:
    # define console handler
    console_handler = logging.StreamHandler(sys.stdout)

    # set file handler parameters
    console_handler.setFormatter(FORMATTER)
    console_handler.setLevel(logging.INFO)

    return console_handler


def get_file_handler() -> RotatingFileHandler:
    # define file handler
    file_handler = RotatingFileHandler(
        "{0}/app.log".format(LOG_PATH), "a+", maxBytes=(50176 * 1), backupCount=2
    )

    # set file handler parameters
    file_handler.setFormatter(FORMATTER)
    file_handler.setLevel(logging.DEBUG)

    return file_handler


def get_basic_config():
    # defines the logger basic config
    logging.basicConfig(
        format=LOG_FORMAT,
        handlers=[
            RotatingFileHandler(
                filename="{0}/app.log".format(LOG_PATH),
                maxBytes=MAX_FILE_SIZE,
                backupCount=10,
            )
        ],
    )


def get_logger(logger_name):
    if not os.path.isdir(LOG_PATH):
        os.mkdir(LOG_PATH)

    # First, create a basic config to prevent:
    #     [Error 32] The process cannot access the file because it is being used by another process
    #     Ref. https://stackoverflow.com/questions/53711553/python-logger-rotatingfilehandler-failing-under-windows
    # This is required to do the file rotation
    get_basic_config()

    # normal setup
    logger = logging.getLogger(logger_name)

    # Better to have too much log than not enough
    logger.setLevel(logging.DEBUG)

    # get handlers
    # logger.addHandler(get_file_handler())
    logger.addHandler(get_console_handler())

    return logger
