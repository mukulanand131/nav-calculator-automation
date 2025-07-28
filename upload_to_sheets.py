import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import base64
import csv

def upload_to_sheets():
    try:
        # 1. Decode credentials
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(base64.b64decode(os.environ['GDRIVE_CREDENTIALS']).decode('utf-8')),
            ['https://spreadsheets.google.com/feeds']
        )
        
        # 2. Connect to sheet
        client = gspread.authorize(creds)
        sheet = client.open("NAV Results").sheet1
        
        # 3. Append CSV data
        with open('nav_comparison.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            if sheet.row_count == 1:  # Empty sheet
                sheet.append_row(headers)
            for row in reader:
                sheet.append_row(row)
                print(f"Appended: {row}")
                
    except Exception as e:
        print(f"❌ Sheets Error: {str(e)}")
        raise

if __name__ == "__main__":
    upload_to_sheets()
