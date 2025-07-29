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
                'date', 'calculation_time', 'calculated_nav', 
                'official_nav', 'difference', 'percentage_diff',
                'fund_name', 'equity_portion'
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
            # Verify headers
            print("CSV Headers:", reader.fieldnames)
            new_rows = list(reader)
            print(f"Found {len(new_rows)} new records in CSV")

        cutoff_time = time(15, 30)  # 3:30 PM
        rows_appended = 0
        rows_skipped = 0

        for row in new_rows:
            # Verify required fields exist
            if not all(k in row for k in ['calculation_time', 'date', 'calculated_nav', 'fund_name']):
                print(f"⚠️ Missing required fields in row: {row}")
                continue

            try:
                # Parse the time from the CSV (use correct field name)
                row_time = datetime.strptime(row['calculation_time'], "%H:%M:%S").time()
            except ValueError as e:
                print(f"⚠️ Could not parse time for row: {row}. Error: {e}")
                continue

            # Only consider rows after 3:30 PM for duplicate check
            if row_time < cutoff_time:
                sheet.append_row([row.get(field, '') for field in [
                    'date', 'calculation_time', 'calculated_nav',
                    'official_nav', 'difference', 'percentage_diff',
                    'fund_name', 'equity_portion'
                ]])
                rows_appended += 1
                print(f"✓ Appended (before cutoff): {row['date']} {row['calculation_time']} {row['fund_name']}")
                continue

            # Check for duplicates in existing records
            is_duplicate = False
            for existing in existing_records:
                if (existing.get('date') == row['date'] and
                    existing.get('fund_name') == row['fund_name'] and
                    str(existing.get('calculated_nav')) == str(row['calculated_nav'])):
                    
                    # Parse existing record's time
                    try:
                        existing_time = datetime.strptime(existing.get('calculation_time', ''), "%H:%M:%S").time()
                    except ValueError:
                        continue
                    
                    if existing_time >= cutoff_time:
                        is_duplicate = True
                        break

            if not is_duplicate:
                sheet.append_row([row.get(field, '') for field in [
                    'date', 'calculation_time', 'calculated_nav',
                    'official_nav', 'difference', 'percentage_diff',
                    'fund_name', 'equity_portion'
                ]])
                rows_appended += 1
                print(f"✓ Appended: {row['date']} {row['calculation_time']} {row['fund_name']}")
            else:
                rows_skipped += 1
                print(f"⏩ Skipped duplicate: {row['date']} {row['calculation_time']} {row['fund_name']}")

        print(f"\nUpload complete. Appended {rows_appended} rows, skipped {rows_skipped} duplicates.")
                
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    upload_to_sheets()
