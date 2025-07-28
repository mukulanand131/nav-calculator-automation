import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import base64

def upload_to_sheets():
    try:
        # 1. Decode credentials
        creds_json = base64.b64decode(os.environ['GDRIVE_CREDENTIALS']).decode('utf-8')
        creds_dict = json.loads(creds_json)
        
        # 2. Use correct scopes (add drive scope)
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # 3. Open sheet (add error handling)
        try:
            sheet = client.open("NAV Results").sheet1
        except gspread.SpreadsheetNotFound:
            print("Creating new sheet 'NAV Results'")
            sheet = client.create("NAV Results").sheet1
            sheet.append_row(["Date", "Calc NAV", "Official NAV", "Difference", "% Diff"])
        
        # 4. Append data
        with open('nav_comparison.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                sheet.append_row(row)
                print(f"✓ Appended: {row}")
                
    except Exception as e:
        print(f"❌ Sheets Error: {str(e)}")
        raise

if __name__ == "__main__":
    upload_to_sheets()
