import os
import ftplib
import logging
import time
from dotenv import load_dotenv, find_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv(find_dotenv())

host = os.environ["host"]
username = os.environ["user"]
password = os.environ["passwd"]

class FtpHandler:
    def __init__(self):
        self.conn = ftplib.FTP(host)
        self.conn.login(user=username, passwd=password)
        
    def upload_files(self, output_folder: str, filename: str) -> None:
        local_file_path = os.path.join(output_folder, filename)

        if os.path.isfile(local_file_path):
            try:
                with open(local_file_path, 'rb') as file:
                    self.conn.storbinary(f'STOR {filename}', file)
                logging.info(f"Uploaded: {filename}")
            except Exception as error:
                logging.info(f"Couldn't upload: {filename}")
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
        logging.info(f"All files were uploaded in parallel via FTP: {execution_time}min")

