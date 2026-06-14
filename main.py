import argparse
from datetime import datetime, timedelta
from utils import get_all_ghl_contacts, get_ghl_contact, send_ghl_message, add_ghl_tag

MOVE_DATE_FIELD_ID = "VuatzebiX5qPrzGjl4d4"
BAD_MOVE_FIELD_ID  = "cf9E3HWw8Qnoh6Xze7ph"
CONTACTED_TAG      = "reactivation_campaign"
BATCH_SIZE         = 50

SMS_BODY_TEMPLATE = """Hey {first_name}! It's Jim from Splendid Moving, we helped you move a while back.

Wanted to check in — are you or anyone you know planning a move soon? We'd love to help again, and I can get you 5% off as a returning client — same goes for anyone you refer.

Just shoot me a text or a call back!"""


def is_bad_move(value):
    if isinstance(value, list):
        return any(isinstance(v, str) and v.strip() for v in value)
    if isinstance(value, str):
        return value.strip().lower() not in ("", "false", "no")
    return False


def parse_move_date(value):
    try:
        if isinstance(value, (int, float)):
            return datetime.utcfromtimestamp(value / 1000).date()
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (ValueError, OSError):
        return None


def in_window(value):
    move_date = parse_move_date(value)
    if not move_date:
        return False
    today = datetime.now().date()
    return today - timedelta(days=365) <= move_date <= today - timedelta(days=180)


def already_contacted(contact):
    tags = [t.lower() for t in (contact.get("tags") or [])]
    return CONTACTED_TAG in tags


def run_job(dry_run=False):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Starting reactivation campaign (dry_run={dry_run})")

    contacts = get_all_ghl_contacts()
    print(f"Total contacts: {len(contacts)}")

    sent = skipped_tag = skipped_bad_move = skipped_window = skipped_no_date = 0

    for contact in contacts:
        if sent >= BATCH_SIZE:
            print(f"Reached batch limit of {BATCH_SIZE}.")
            break

        contact_id = contact.get("id")
        first_name = (contact.get("firstName") or "there").title()

        # Cheap filters from list API
        custom_fields = contact.get("customFields", [])
        move_date_value = bad_move_value = None
        for field in custom_fields:
            fid = field.get("id")
            if fid == MOVE_DATE_FIELD_ID:
                move_date_value = field.get("value")
            elif fid == BAD_MOVE_FIELD_ID:
                bad_move_value = field.get("value")

        if is_bad_move(bad_move_value):
            skipped_bad_move += 1
            continue
        if move_date_value is None:
            skipped_no_date += 1
            continue
        if not in_window(move_date_value):
            skipped_window += 1
            continue

        # Tag check requires individual fetch (list API doesn't return tags)
        full_contact = get_ghl_contact(contact_id)
        if full_contact and already_contacted(full_contact):
            skipped_tag += 1
            continue

        print(f"  → {first_name} ({contact_id}) moved {parse_move_date(move_date_value)}")

        if not dry_run:
            try:
                send_ghl_message(contact_id, "SMS", SMS_BODY_TEMPLATE.format(first_name=first_name))
                add_ghl_tag(contact_id, CONTACTED_TAG)
                sent += 1
            except Exception as e:
                print(f"  ERROR {contact_id}: {e}")
        else:
            sent += 1

    print(
        f"Done. Sent: {sent} | Already contacted: {skipped_tag} | "
        f"Bad move: {skipped_bad_move} | Outside window: {skipped_window} | No date: {skipped_no_date}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_job(dry_run=args.dry_run)
