import os
import argparse
from datetime import datetime, timedelta
from utils import GoogleSheetClient, send_ghl_message, send_notification, get_ghl_contact

# Configuration
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TAB_NAME = "Sheet1"
BATCH_SIZE = 30
DELAY_HOURS = 24

# Column Indices (Assumed based on 'get_rows' output: 
# ['Full name', 'First name', 'Email', 'Phone', 'Move date', 'Movers', 'Rate', 'Contact ID', 'Status']
# Status is index 8 (Column I).
# We will use index 9 (Column J) for 'Date Email Sent'.
STATUS_COL_IDX = 8
DATE_SENT_COL_IDX = 9
CONTACT_ID_COL_IDX = 7
FIRST_NAME_COL_IDX = 1

SMS_BODY_TEMPLATE = """Hey {first_name}! It's Jim from Splendid Moving, we helped you move a while back.

Wanted to check in — are you or anyone you know planning a move soon? We'd love to help again, and I can get you 5% off as a returning client — same goes for anyone you refer.

Just shoot me a text or a call back!"""

def get_status(row):
    if len(row) > STATUS_COL_IDX:
        return row[STATUS_COL_IDX].strip()
    return ""

def get_date_sent(row):
    if len(row) > DATE_SENT_COL_IDX:
        return row[DATE_SENT_COL_IDX].strip()
    return None

def main(dry_run=False):
    print(f"Starting Loyalty Flow (Dry Run: {dry_run})")
    client = GoogleSheetClient(SPREADSHEET_ID)
    rows = client.read_all_rows(TAB_NAME)
    
    if not rows:
        print("No rows found.")
        return

    data_rows = rows[1:]
    
    # Start SMS Campaign
    print("Starting SMS Campaign...")
    new_batch_count = 0
    new_batch_indices = []
    
    for i, row in enumerate(data_rows):
        status = get_status(row)
        if status == "New" or status == "":
            new_batch_indices.append(i + 1) # +1 for header
    
    if not new_batch_indices:
        print("No new contacts found to process.")
        return
        
    print(f"Found {len(new_batch_indices)} possible new contacts. Will attempt to send {BATCH_SIZE} messages.")
    
    for row_idx in new_batch_indices:
        if new_batch_count >= BATCH_SIZE:
            print(f"Reached daily target of {BATCH_SIZE} sent messages.")
            break
            
        row = rows[row_idx]
        contact_id = row[CONTACT_ID_COL_IDX] if len(row) > CONTACT_ID_COL_IDX else None
        first_name = row[FIRST_NAME_COL_IDX] if len(row) > FIRST_NAME_COL_IDX else "there"
        
        if not contact_id:
            print(f"Row {row_idx}: Missing Contact ID. Skipping.")
            continue
            
        print(f"Row {row_idx}: Process contact {first_name} ({contact_id})")
        
        # Check GHL fields: Move Date > 6 months ago, and Bad Move is not checked
        contact = get_ghl_contact(contact_id)
        if contact:
            custom_fields = contact.get("customFields", [])
            move_date_str = None
            bad_move_value = None
            
            for field in custom_fields:
                if field.get("id") == "VuatzebiX5qPrzGjl4d4": # Move Date
                    move_date_str = field.get("value")
                elif field.get("id") == "cf9E3HWw8Qnoh6Xze7ph": # Mark if the move went bad
                    bad_move_value = field.get("value")
            
            # 1. Check if 'Bad Move' is checked
            is_bad_move = False
            if isinstance(bad_move_value, list):
                # Filter out empty strings
                meaningful_values = [v for v in bad_move_value if isinstance(v, str) and v.strip()]
                if len(meaningful_values) > 0:
                    is_bad_move = True
            elif isinstance(bad_move_value, str) and bad_move_value.strip().lower() not in ["", "false", "no"]:
                is_bad_move = True
                
            if is_bad_move:
                print(f"Row {row_idx}: Skipped. Contact has 'bad move' checked.")
                if not dry_run:
                    client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "Skipped (Bad Move)")
                continue

            # 2. Check if Move Date is at least 6 months ago (180 days)
            if move_date_str:
                try:
                    move_date = datetime.strptime(move_date_str, "%Y-%m-%d").date()
                    six_months_ago = datetime.now().date() - timedelta(days=180)
                    if move_date > six_months_ago:
                        print(f"Row {row_idx}: Skipped. Move date ({move_date_str}) is not at least 6 months ago.")
                        if not dry_run:
                            client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "Skipped (Recent/Future Move)")
                        continue
                except ValueError:
                    print(f"Row {row_idx}: Could not parse move date '{move_date_str}'. Proceeding normally.")
            else:
                # If there's no move date in GHL, skip just to be safe according to requirements
                print(f"Row {row_idx}: Skipped. Contact has no move date set in GHL.")
                if not dry_run:
                    client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "Skipped (No Move Date)")
                continue
        else:
            print(f"Row {row_idx}: Could not fetch contact details from GHL. Proceeding normally.")
            
        print(f"Row {row_idx}: Sending SMS to {first_name} ({contact_id})")
        msg_body = SMS_BODY_TEMPLATE.format(first_name=first_name)
        
        if not dry_run:
            try:
                send_ghl_message(contact_id, "SMS", msg_body)
                client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "Done")
                new_batch_count += 1
            except Exception as e:
                print(f"Failed to send SMS for row {row_idx}: {e}")
                # Mark as failed so it doesn't block the pipeline
                client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "SMS Failed")
                print(f"Row {row_idx}: Marked as 'SMS Failed' to unblock pipeline.")
        else:
            new_batch_count += 1

    # Flush all queued sheet updates in a single API call
    if not dry_run:
        client.flush_updates()

    if new_batch_count > 0:
        msg = f"Sent SMS to {new_batch_count} contacts."
        print(msg)
        if not dry_run:
            send_notification(msg)

def run_job():
    main(dry_run=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run once without sending messages")
    args = parser.parse_args()
    
    main(dry_run=args.dry_run)
