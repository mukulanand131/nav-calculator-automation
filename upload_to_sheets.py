import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import base64
import csv
from datetime import datetime, time

def upload_to_sheets():
    print("\n=== Google Sheets Upload ===")
    try:
        # 1. Decode credentials
        creds_json = base64.b64decode(os.environ['GDRIVE_CREDENTIALS']).decode('utf-8')
        creds_dict = json.loads(creds_json)
        
        # 2. Authenticate with correct scopes
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        print("✓ Authentication successful")

        # 3. Open or create sheet
        try:
            sheet = client.open("NAV Results").sheet1
            print("✓ Found existing sheet")
        except gspread.SpreadsheetNotFound:
            print("Creating new sheet 'NAV Results'")
            sheet = client.create("NAV Results").sheet1
            sheet.append_row([
                'Date', 'Time', 'Calculated NAV', 
                'Official NAV', 'Difference', '% Diff',
                'Fund Name', 'Equity Portion'
            ])
            print("✓ Created new sheet with headers")
            existing_records = []
        else:
            # Get all existing records if sheet exists
            existing_records = sheet.get_all_records()
            print(f"✓ Loaded {len(existing_records)} existing records")

        # 4. Process CSV data
        with open('nav_comparison.csv', 'r') as f:
            reader = csv.DictReader(f)
            new_rows = list(reader)
            print(f"Found {len(new_rows)} new records in CSV")

        cutoff_time = time(15, 30)  # 3:30 PM
        rows_appended = 0
        rows_skipped = 0

        for row in new_rows:
            # Parse the time from the CSV
            try:
                row_time = datetime.strptime(row['Time'], "%H:%M:%S").time()
            except ValueError:
                print(f"⚠️ Could not parse time for row: {row}")
                continue

            # Only consider rows after 3:30 PM for duplicate check
            if row_time < cutoff_time:
                sheet.append_row(list(row.values()))
                rows_appended += 1
                print(f"✓ Appended (before cutoff): {row['Date']} {row['Time']} {row['Fund Name']}")
                continue

            # Check for duplicates in existing records
            is_duplicate = False
            for existing in existing_records:
                if (existing['Date'] == row['Date'] and
                    existing['Fund Name'] == row['Fund Name'] and
                    float(existing['Calculated NAV']) == float(row['Calculated NAV'])):
                    
                    # Parse existing record's time
                    try:
                        existing_time = datetime.strptime(existing['Time'], "%H:%M:%S").time()
                    except ValueError:
                        continue
                    
                    if existing_time >= cutoff_time:
                        is_duplicate = True
                        break

            if not is_duplicate:
                sheet.append_row(list(row.values()))
                rows_appended += 1
                print(f"✓ Appended: {row['Date']} {row['Time']} {row['Fund Name']}")
            else:
                rows_skipped += 1
                print(f"⏩ Skipped duplicate: {row['Date']} {row['Time']} {row['Fund Name']}")

        print(f"\nUpload complete. Appended {rows_appended} rows, skipped {rows_skipped} duplicates.")
                
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    upload_to_sheets()
