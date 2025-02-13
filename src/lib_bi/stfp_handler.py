import os
import pysftp
import logging
import time
from dotenv import load_dotenv, find_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv(find_dotenv())

host = os.environ["host"]
username = os.environ["user"]
password = os.environ["passwd"]


class SftpHandler:
    def __init__(self):
        self.conn = pysftp.Connection(host=host, username=username, password=password)

    def upload_files(self, output_folder: str, filename: str) -> None:
        local_file_path = os.path.join(output_folder, filename)

        # Check if it's a file (not a directory)
        if os.path.isfile(local_file_path):
            try:
                # Upload the file
                self.conn.put(local_file_path)
                logging.info(f"Uploaded: {filename}")
            except Exception as error:
                logging.info(f"Couldnt upload: {filename}")
                raise error

    def upload_all_files_in_parallel(self, output_folder: str):
        start = time.time()
        files_name_to_upload = os.listdir(output_folder)
        with ThreadPoolExecutor() as pool:
            pool.map(
                lambda item: self.upload_files(output_folder, item),
                files_name_to_upload,
            )

        logging.info("All files loaded successfully :3")
        end = time.time()
        execution_time = round((end - start)/60)
        logging.info(f"All files were upload in paralel by through sftp: {execution_time}min")
