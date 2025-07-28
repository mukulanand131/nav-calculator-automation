import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import base64
from datetime import datetime

def upload_to_google_sheets():
    try:
        # 1. Decode credentials from GitHub Secrets
        creds_json = base64.b64decode(os.environ['GDRIVE_CREDENTIALS']).decode('utf-8')
        creds_dict = json.loads(creds_json)
        
        # 2. Authenticate
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # 3. Open sheet and append data
        sheet = client.open("NAV Results").sheet1
        
        # Read CSV and upload
        with open('nav_comparison.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                sheet.append_row(row)
                print(f"Uploaded row: {row}")
                
    except Exception as e:
        print(f"Google Sheets Error: {str(e)}")

if __name__ == "__main__":
    upload_to_google_sheets()
