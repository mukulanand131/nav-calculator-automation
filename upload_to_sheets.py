import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import base64
import csv
from datetime import datetime, time, timedelta
import pytz  # Add this import

def get_local_time():
    """Get current time in IST timezone"""
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

def upload_to_sheets():
    print("\n=== Google Sheets Upload ===")
    try:
        # 1. Decode credentials with better error handling
        try:
            creds_json = base64.b64decode(os.environ['GDRIVE_CREDENTIALS']).decode('utf-8')
            creds_dict = json.loads(creds_json)
        except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Invalid credentials: {str(e)}")

        # 2. Authenticate with correct scopes
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            print("✓ Authentication successful")
        except Exception as e:
            raise ConnectionError(f"Authentication failed: {str(e)}")

        # 3. Open or create sheet with better error handling
        try:
            sheet = client.open("NAV Results").sheet1
            print("✓ Found existing sheet")
            
            # Get existing records more efficiently
            existing_records = sheet.get_all_records()
            print(f"✓ Loaded {len(existing_records)} existing records")
        except gspread.SpreadsheetNotFound:
            print("Creating new sheet 'NAV Results'")
            try:
                sheet = client.create("NAV Results").sheet1
                sheet.append_row([
                    'date', 'calculation_time', 'calculated_nav', 
                    'official_nav', 'difference', 'percentage_diff',
                    'fund_name', 'equity_portion'
                ])
                print("✓ Created new sheet with headers")
                existing_records = []
            except Exception as e:
                raise RuntimeError(f"Failed to create sheet: {str(e)}")

        # 4. Process CSV data with better error handling
        try:
            with open('nav_comparison.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                new_rows = list(reader)
                print(f"Found {len(new_rows)} new records in CSV")
        except FileNotFoundError:
            raise FileNotFoundError("CSV file not found")
        except Exception as e:
            raise IOError(f"Error reading CSV: {str(e)}")

        # Timezone-aware cutoff time (3:30 PM IST)
        cutoff_time = time(15, 30)
        ist = pytz.timezone('Asia/Kolkata')
        today = get_local_time().date()
        yesterday = today - timedelta(days=1)

        rows_appended = 0
        rows_skipped = 0
        rows_updated = 0

        for row in new_rows:
            # Validate required fields
            required_fields = ['date', 'calculation_time', 'calculated_nav', 'fund_name']
            if not all(field in row for field in required_fields):
                print(f"⚠️ Missing required fields in row: {row}")
                continue

            try:
                # Parse date and time with timezone awareness
                row_date = datetime.strptime(row['date'], "%d/%m/%Y").date()
                row_time = datetime.strptime(row['calculation_time'], "%H:%M:%S").time()
                
                # Create timezone-aware datetime for comparison
                row_datetime = ist.localize(
                    datetime.combine(row_date, row_time)
                
                # Check if this is an update to yesterday's official NAV
                if (row_date == yesterday and 
                    row.get('official_nav') and 
                    float(row['official_nav']) > 0):
                    
                    # Find matching record to update
                    for i, existing in enumerate(existing_records, start=2):  # row 2 is first data row
                        if (existing.get('date') == row['date'] and 
                            existing.get('fund_name') == row['fund_name']):
                            
                            # Update the existing record
                            sheet.update_cell(i, 4, row['official_nav'])  # Column 4 is official_nav
                            if existing.get('calculated_nav'):
                                try:
                                    diff = float(row['official_nav']) - float(existing['calculated_nav'])
                                    sheet.update_cell(i, 5, round(diff, 4))  # Column 5 is difference
                                    perc_diff = (diff / float(existing['calculated_nav'])) * 100
                                    sheet.update_cell(i, 6, round(perc_diff, 4))  # Column 6 is percentage_diff
                                except (ValueError, TypeError):
                                    pass
                            rows_updated += 1
                            print(f"↻ Updated official NAV for {row['date']} {row['fund_name']}")
                            break
                    continue

                # For new entries, check for duplicates after cutoff time
                is_duplicate = False
                if row_time >= cutoff_time:
                    for existing in existing_records:
                        if (existing.get('date') == row['date'] and 
                            existing.get('fund_name') == row['fund_name'] and 
                            str(existing.get('calculated_nav')) == str(row['calculated_nav'])):
                            
                            try:
                                existing_time = datetime.strptime(
                                    existing.get('calculation_time', '00:00:00'), 
                                    "%H:%M:%S").time()
                            except ValueError:
                                continue
                            
                            if existing_time >= cutoff_time:
                                is_duplicate = True
                                break

                if not is_duplicate:
                    # Prepare row data ensuring all fields are present
                    row_data = [
                        row.get('date', ''),
                        row.get('calculation_time', ''),
                        row.get('calculated_nav', ''),
                        row.get('official_nav', ''),
                        row.get('difference', ''),
                        row.get('percentage_diff', ''),
                        row.get('fund_name', ''),
                        row.get('equity_portion', '')
                    ]
                    sheet.append_row(row_data)
                    rows_appended += 1
                    print(f"✓ Appended: {row['date']} {row['calculation_time']} {row['fund_name']}")
                else:
                    rows_skipped += 1
                    print(f"⏩ Skipped duplicate: {row['date']} {row['fund_name']}")

            except Exception as e:
                print(f"⚠️ Error processing row {row}: {str(e)}")
                continue

        print(f"\nUpload summary:")
        print(f"- Appended: {rows_appended} new rows")
        print(f"- Updated: {rows_updated} official NAVs")
        print(f"- Skipped: {rows_skipped} duplicates")
                
    except Exception as e:
        print(f"❌ Critical Error: {str(e)}")
        raise

if __name__ == "__main__":
    upload_to_sheets()
