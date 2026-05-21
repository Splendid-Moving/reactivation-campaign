import os
import json
import base64
import time
import requests
from dotenv import load_dotenv, find_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv(find_dotenv())

GHL_ACCESS_TOKEN = os.getenv("GHL_ACCESS_TOKEN")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

BASE_URL = "https://services.leadconnectorhq.com"
HEADERS = {
    "Authorization": f"Bearer {GHL_ACCESS_TOKEN}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json"
}


def decode_service_account():
    if not SERVICE_ACCOUNT_JSON:
        if os.path.exists("service_account.json"):
            with open("service_account.json", "r") as f:
                return json.load(f)
        raise ValueError("Missing SERVICE_ACCOUNT_JSON env var and service_account.json file.")
    try:
        return json.loads(base64.b64decode(SERVICE_ACCOUNT_JSON).decode("utf-8"))
    except Exception as e:
        try:
            return json.loads(SERVICE_ACCOUNT_JSON)
        except:
            raise ValueError(f"Failed to decode SERVICE_ACCOUNT_JSON: {e}")


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_info(
        decode_service_account(),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


def get_ghl_contact(contact_id):
    response = requests.get(f"{BASE_URL}/contacts/{contact_id}", headers=HEADERS)
    if response.status_code == 200:
        return response.json().get("contact")
    return None


def get_all_ghl_contacts():
    contacts = []
    params = {"locationId": GHL_LOCATION_ID, "limit": 100}

    while True:
        response = requests.get(f"{BASE_URL}/contacts/", headers=HEADERS, params=params)
        if response.status_code == 429:
            time.sleep(2)
            continue
        if response.status_code != 200:
            print(f"Error fetching contacts: {response.status_code} {response.text}")
            break

        data = response.json()
        page = data.get("contacts", [])
        contacts.extend(page)

        meta = data.get("meta", {})
        start_after_id = meta.get("startAfterId")
        if not start_after_id or len(page) < 100:
            break

        params["startAfterId"] = start_after_id
        params["startAfter"] = meta.get("startAfter")

    return contacts


def add_ghl_tag(contact_id, tag):
    response = requests.post(f"{BASE_URL}/contacts/{contact_id}/tags", headers=HEADERS, json={"tags": [tag]})
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to add tag to {contact_id}: {response.text}")
    return response.json()


def send_ghl_message(contact_id, message_type, content, subject=None):
    payload = {"contactId": contact_id, "type": message_type}
    if message_type.lower() == "email":
        if not subject:
            raise ValueError("Subject is required for emails.")
        payload["subject"] = subject
        payload["html"] = content
    else:
        payload["message"] = content

    response = requests.post(f"{BASE_URL}/conversations/messages", headers=HEADERS, json=payload)
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to send {message_type}: {response.text}")
    return response.json()


class GoogleSheetClient:
    def __init__(self, spreadsheet_id):
        self.service = get_sheets_service()
        self.spreadsheet_id = spreadsheet_id
        self._pending_updates = []

    def read_all_rows(self, tab_name="Sheet1"):
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=f"{tab_name}!A:Z"
        ).execute()
        return result.get("values", [])

    def _col_letter(self, col_index):
        return chr(ord("A") + col_index)

    def queue_update(self, tab_name, row_index, col_index, value):
        col_letter = self._col_letter(col_index)
        self._pending_updates.append({
            "range": f"{tab_name}!{col_letter}{row_index + 1}",
            "values": [[value]]
        })

    def flush_updates(self):
        if not self._pending_updates:
            return
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"valueInputOption": "RAW", "data": self._pending_updates}
        ).execute()
        print(f"Flushed {len(self._pending_updates)} cell updates.")
        self._pending_updates = []

    def update_cell(self, tab_name, row_index, col_index, value):
        col_letter = self._col_letter(col_index)
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{tab_name}!{col_letter}{row_index + 1}",
            valueInputOption="RAW",
            body={"values": [[value]]}
        ).execute()

    def update_status(self, tab_name, row_index, status_col_index, status, date_col_index=None, date_val=None):
        self.queue_update(tab_name, row_index, status_col_index, status)
        if date_col_index is not None and date_val is not None:
            self.queue_update(tab_name, row_index, date_col_index, date_val)
