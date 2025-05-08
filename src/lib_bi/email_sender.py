import smtplib
import os
import base64
from googleapiclient.discovery import build
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from google.auth.credentials import Credentials
import pickle


SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

def send_email(
    to_addrs: str | list[str],
    subject: str,
    body_text: str,
    files: str | list[str],
    email_address: str,
    email_password: str,
    smtp_server: str = "",
    smtp_port: int = 465,
) -> None:
    if not isinstance(to_addrs, list):
        to_addrs = [to_addrs]

    if not isinstance(files, list):
        files = [files]

    from_addr = email_address

    message = MIMEMultipart()
    message["Subject"] = subject
    message["From"] = from_addr
    message["To"] = ", ".join(to_addrs)
    message.attach(MIMEText(body_text))

    with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(from_addr, email_password)
        server.sendmail(from_addr, to_addrs, message.as_string())

def authorize(path_mail_json: str) -> Credentials:
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(path_mail_json, SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return creds


def send_email_via_gmail(
    to: str, subject: str, body_text: str, file_path: str, path_mail_json: str
) -> None:
    creds = authorize(path_mail_json)
    service = build("gmail", "v1", credentials=creds)

    message = MIMEMultipart()
    message["to"] = to
    message["from"] = ""
    message["subject"] = subject

    msg = MIMEText(body_text)
    message.attach(msg)

    if not os.path.exists(file_path):
        print(f"NO DATA: {to}")
        return None

    with open(file_path, "rb") as file:
        file_name = os.path.basename(file_path)
        message.attach(MIMEApplication(file.read(), Name=file_name))

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    body = {"raw": raw_message}

    try:
        message = service.users().messages().send(userId="me", body=body).execute()
        print(f'Message Id: {message["id"]}')
        return message
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None
