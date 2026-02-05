import os
import json
import base64
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime

# Environment Variables
GHL_ACCESS_TOKEN = os.getenv("GHL_ACCESS_TOKEN")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
MAKE_WEBHOOK_URL = "https://hook.us2.make.com/l6ahmuiwmdo2wvl7h3b4yh8h5wse5y68"

# GHL Constants
BASE_URL = "https://services.leadconnectorhq.com"
HEADERS = {
    "Authorization": f"Bearer {GHL_ACCESS_TOKEN}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def decode_service_account():
    """Decodes the Base64 Service Account JSON."""
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON environment variable.")
    
    # Check if it's already a dict (development fallback) or needs decoding
    try:
        decoded_bytes = base64.b64decode(SERVICE_ACCOUNT_JSON)
        decoded_str = decoded_bytes.decode("utf-8")
        return json.loads(decoded_str)
    except Exception as e:
        # Fallback: maybe it's just the raw json string?
        try:
           return json.loads(SERVICE_ACCOUNT_JSON)
        except:
           raise ValueError(f"Failed to decode SERVICE_ACCOUNT_JSON: {e}")

def get_sheets_service():
    """Authenticates and returns the Google Sheets service object."""
    creds_dict = decode_service_account()
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds)

def send_notification(message):
    """Sends a notification to the Make.com webhook."""
    payload = {"message": message}
    try:
        requests.post(MAKE_WEBHOOK_URL, json=payload)
    except Exception as e:
        print(f"Failed to send notification: {e}")

def get_ghl_contact(contact_id):
    """Fetch contact details from GHL."""
    url = f"{BASE_URL}/contacts/{contact_id}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json().get('contact')
    return None

def send_ghl_message(contact_id, message_type, content, subject=None):
    """Sends a message via GHL."""
    url = f"{BASE_URL}/conversations/messages"
    payload = {
        "contactId": contact_id,
        "type": message_type,
    }
    
    if message_type.lower() == "email":
        if not subject:
            raise ValueError("Subject is required for emails.")
        payload["subject"] = subject
        payload["html"] = content
    else:
        payload["message"] = content

    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to send {message_type}: {response.text}")
    return response.json()

class GoogleSheetClient:
    def __init__(self, spreadsheet_id):
        self.service = get_sheets_service()
        self.spreadsheet_id = spreadsheet_id
        
    def read_all_rows(self, tab_name="Sheet1"):
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=f"{tab_name}!A:Z"
        ).execute()
        return result.get('values', [])
        
    def update_cell(self, tab_name, row_index, col_index, value):
        """Updates a single cell. row_index is 0-based, col_index is 0-based."""
        # Convert row/col to A1 notation
        # Start row is row_index + 1
        # Col A=0, B=1, ... J=9 (Date Sent is likely J if Status is I?)
        
        # Helper to convert col index to letter
        col_letter = chr(ord('A') + col_index)
        cell_range = f"{tab_name}!{col_letter}{row_index + 1}"
        
        body = {'values': [[value]]}
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=cell_range,
            valueInputOption='RAW',
            body=body
        ).execute()

    def update_status(self, tab_name, row_index, status_col_index, status, date_col_index=None, date_val=None):
        """Updates status and optionally the date."""
        self.update_cell(tab_name, row_index, status_col_index, status)
        if date_col_index is not None and date_val is not None:
             self.update_cell(tab_name, row_index, date_col_index, date_val)
