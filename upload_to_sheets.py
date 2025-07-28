import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import base64
import csv
from datetime import datetime

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

        # 4. Append CSV data
        with open('nav_comparison.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                sheet.append_row(row)
                print(f"✓ Appended: {row[:3]}...")  # Truncate for logs
                
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    upload_to_sheets()
