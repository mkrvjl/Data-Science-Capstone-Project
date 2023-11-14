import hashlib
import os.path
import urllib
import os.path as path
import time
from datetime import datetime

import config.logs as logs

logger = logs.get_logger(__name__)


def validate_source_files(url: str, file_path: str):
    local_hash = None
    # assess if file exists, hash contents
    if os.path.exists(file_path):
        # assess local file last modification
        if not is_update_required(file_path=file_path):
            return

        # read file contents
        with open(file_path, "rb") as j:
            data = j.read()

        # hash local file
        local_hash = hashlib.md5(data).hexdigest()

    # prepare request
    web_url = urllib.request.Request(url)
    web_url.add_header("User-Agent", "Mozilla/5.0")

    # read remote file content
    with urllib.request.urlopen(web_url) as downloaded_file:
        # read and hash remote file
        remote_data = downloaded_file.read()
        remote_hash = hashlib.md5(remote_data).hexdigest()

        # remote file has been changed, update local file
        if local_hash != remote_hash:
            # save file
            save_file(data=remote_data, file_path=file_path)
            logger.debug("File updated...")
        else:
            # force modified time update
            date = datetime.now()
            mod_time = time.mktime(date.timetuple())
            os.utime(file_path, (mod_time, mod_time))
            logger.debug("File is already up-to-date...")


def save_file(data, file_path: str):
    # create folder for filepath if it does not exist
    root_dir = os.path.dirname(file_path)

    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
        logger.debug(f"Directory ({root_dir}) created...")

    # write binary file
    with open(file_path, "wb") as out_file:
        out_file.write(data)

    # confirm that file is written and ready
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} was not found!")

    with open(file_path, "r") as file:
        if file.readable():
            logger.info(f"File '{file_path}' downloaded and ready...")
        else:
            raise IOError(f"Unable to read downloaded file '{file_path}'")


def is_update_required(file_path: str, days_since_last_update: int = 7):
    # get last modified time
    last_modified = path.getmtime(file_path)

    # is last update more than 7 days?
    if (time.time() - last_modified) / 3600 > 24 * days_since_last_update:
        # yes, update
        is_required = True
        logger.info(
            f"File ({file_path}) is older than "
            f"({days_since_last_update}) days, updating..."
        )

    else:
        # no, update
        is_required = False
        logger.debug(
            f"File '{file_path}' is not older " f"enough to update. Skipping..."
        )

    return is_required
