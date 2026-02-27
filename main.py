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

# Messages
EMAIL_SUBJECT = "Important Update"
EMAIL_BODY_TEMPLATE = """<div style="font-size: 16px; font-family: Arial, sans-serif; color: #333;">
<p>Hi {first_name},</p>

<p>It’s been a while since we saw you! I was updating our client records and realized you qualify for our "Returning Client" status.</p>

<p>That means if you move with us again, you automatically get <strong>5% off</strong> your quote.</p>

<p>We just opened up a few spots for estimates next week. Are you moving soon (or know someone who is)?</p>

<p>Best,<br>Jim</p>
</div>"""

SMS_BODY_TEMPLATE = """Hey {first_name}, Jim here from Splendid Moving.

Did you see my email about the 5% loyalty discount?

If you are moving soon, you can use this link to get a quote with the 5% off applied: https://services.msgsndr.com/urls/l/QGRUkVTzw 

Let me know if it works!"""

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
    
    # 1. Check for Active Batch (Status = 'Email Sent')
    active_batch_indices = []
    for i, row in enumerate(data_rows):
        if get_status(row) == "Email Sent":
            active_batch_indices.append(i + 1) # +1 for header offset

    if active_batch_indices:
        print(f"Found active batch: {len(active_batch_indices)} contacts.")
        processed_count = 0
        
        for row_idx in active_batch_indices:
            row = rows[row_idx]
            contact_id = row[CONTACT_ID_COL_IDX] if len(row) > CONTACT_ID_COL_IDX else None
            date_sent_str = get_date_sent(row)
            first_name = row[FIRST_NAME_COL_IDX] if len(row) > FIRST_NAME_COL_IDX else "there"
            
            if not contact_id:
                print(f"Row {row_idx}: Missing Contact ID. Skipping.")
                continue

            if not date_sent_str:
                print(f"Row {row_idx}: Status is 'Email Sent' but no date found. Skipping.")
                continue
                
            try:
                date_sent = datetime.fromisoformat(date_sent_str)
            except ValueError:
                print(f"Row {row_idx}: Invalid date format '{date_sent_str}'. Skipping.")
                continue
                
            # Check 48 hours
            time_diff = datetime.now() - date_sent
            if time_diff < timedelta(hours=DELAY_HOURS):
                print(f"Row {row_idx}: Only {time_diff} has passed. Waiting.")
                continue
                
            # Send SMS
            print(f"Row {row_idx}: Sending SMS to {first_name} ({contact_id})")
            msg_body = SMS_BODY_TEMPLATE.format(first_name=first_name)
            
            if not dry_run:
                try:
                    send_ghl_message(contact_id, "SMS", msg_body)
                    client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "Done")
                    processed_count += 1
                except Exception as e:
                    print(f"Failed to send SMS for row {row_idx}: {e}")
                    # Mark as failed so it doesn't block the pipeline
                    client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "SMS Failed")
                    print(f"Row {row_idx}: Marked as 'SMS Failed' to unblock pipeline.")
            else:
                processed_count += 1

        if processed_count > 0:
            msg = f"Completed SMS batch for {processed_count} contacts."
            print(msg)
            if not dry_run:
                send_notification(msg)
                
    else:
        # 2. No Active Batch -> Start New Batch
        print("No active batch found. Starting new batch...")
        new_batch_count = 0
        new_batch_indices = []
        
        for i, row in enumerate(data_rows):
            status = get_status(row)
            if status == "New" or status == "":
                new_batch_indices.append(i + 1) # +1 for header
                if len(new_batch_indices) >= BATCH_SIZE:
                    break
        
        if not new_batch_indices:
            print("No new contacts found to process.")
            return
            
        print(f"Targeting {len(new_batch_indices)} new contacts.")
        
        for row_idx in new_batch_indices:
            row = rows[row_idx]
            contact_id = row[CONTACT_ID_COL_IDX] if len(row) > CONTACT_ID_COL_IDX else None
            first_name = row[FIRST_NAME_COL_IDX] if len(row) > FIRST_NAME_COL_IDX else "there"
            
            if not contact_id:
                print(f"Row {row_idx}: Missing Contact ID. Skipping.")
                continue
                
            print(f"Row {row_idx}: Sending Email to {first_name} ({contact_id})")
            email_body = EMAIL_BODY_TEMPLATE.format(first_name=first_name)
            
            if not dry_run:
                try:
                    send_ghl_message(contact_id, "Email", email_body, subject=EMAIL_SUBJECT)
                    current_time = datetime.now().isoformat()
                    client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "Email Sent", DATE_SENT_COL_IDX, current_time)
                    new_batch_count += 1
                except Exception as e:
                    print(f"Failed to send Email for row {row_idx}: {e}")
                    # Mark as failed so it doesn't block the pipeline
                    client.update_status(TAB_NAME, row_idx, STATUS_COL_IDX, "Email Failed")
                    print(f"Row {row_idx}: Marked as 'Email Failed' to unblock pipeline.")
            else:
                new_batch_count += 1
                
        if new_batch_count > 0:
            msg = f"Started new batch. Sent Email to {new_batch_count} contacts."
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
