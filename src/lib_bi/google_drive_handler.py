import io
import os
import json
import polars as pl
import logging
import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build


class GoogleDriveHandler:
    def __init__(self, credentials_env: str):
        self.service_account_info = json.loads(credentials_env)

        self.creds = service_account.Credentials.from_service_account_info(
            self.service_account_info
        )

        self.service = build("drive", "v3", credentials=self.creds)
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        self.gc = gspread.auth.service_account_from_dict(
            self.service_account_info, self.scopes
        )

    def list_csv_files(self, folder_id):
        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = self.service.files().list(q=query).execute()
        items = results.get("files", [])
        return items

    def list_sheet_files(self, folder_id):
        query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"
        results = self.service.files().list(q=query).execute()
        items = results.get("files", [])
        return items

    def download_csv_to_df(self, file_id) -> pl.DataFrame:
        request = self.service.files().get_media(fileId=file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")

        fh.seek(0)
        return pl.read_csv(
            fh,
            encoding="utf8-lossy",
            infer_schema_length=True,
        )

    def extract_sheet(
        self,
        sheet_id: str,
        sheet_name: str,
    ) -> pl.DataFrame:
        data = self.gc.open_by_key(sheet_id).worksheet(sheet_name).get_all_values()

        df = pl.DataFrame(data[1:], schema=data[0])

        return df

    def extract_all_sheet(
        self, drive_folder_id: str, filter_sheets: list
    ) -> pl.DataFrame:
        csv_files = self.list_sheet_files(drive_folder_id)
        dfs = []
        for file in csv_files:
            if file["name"] in filter_sheets:
                continue
            try:
                data = self.gc.open_by_key(file["id"]).sheet1.get_all_values()

            except Exception as error:
                print(file["name"])
                print(error)
                continue
            df = pl.DataFrame(data[1:], schema=data[0])
            last_edited_by = (
                self.service.files()
                .get(fileId=file["id"], fields="lastModifyingUser")
                .execute()["lastModifyingUser"]["displayName"]
            )
            last_edited_time = (
                self.service.files()
                .get(fileId=file["id"], fields="modifiedTime")
                .execute()["modifiedTime"]
            )

            last_edited_time = datetime.strptime(
                last_edited_time, "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            df = df.with_columns(
                pl.lit(last_edited_by).alias("last_edited_by"),
                pl.lit(last_edited_time).alias("last_edited_time"),
            )

            dfs.append(df)
        try:
            df = pl.concat(dfs)
        except Exception as error:
            logging.error(error)
            raise error

        return df

    def upload_df_polars_to_sheet(
        self, df_sheet: pl.DataFrame, sheet_id: str, sheet_name: str
    ) -> None:
        try:
            sheet = self.gc.open_by_key(sheet_id).worksheet(sheet_name)
            try:
                sheet.clear()
                print("Cleaned sheet.")
                # Convert Polars DataFrame to Pandas DataFrame and write to Google Sheet
                df_sheet_pandas = df_sheet.to_pandas()
                set_with_dataframe(sheet, df_sheet_pandas)
                print("DataFrame uploaded to sheet.")
            except Exception as error:
                print("Could not load DataFrame to sheet.")
                raise error

        except Exception as error:
            print(f"An error occurred in the load function: {error}")
            raise error

    def upload_csv_to_drive(self, file_path: str, folder_id: str) -> None:
        file_name = os.path.basename(file_path)
        file_metadata = {"name": file_name, "parents": [folder_id]}

        # Search for existing file
        query = f"name='{file_name}' and '{folder_id}' in parents"
        results = self.service.files().list(q=query, fields="files(id)").execute()
        items = results.get("files", [])

        # Delete existing file if found
        if items:
            for item in items:
                self.service.files().delete(fileId=item["id"]).execute()
                print(f"Deleted existing file: {file_name}")

        media = MediaFileUpload(file_path, mimetype="text/csv")
        file = (
            self.service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        print(f"File ID: {file.get('id')} uploaded to folder ID: {folder_id}")
