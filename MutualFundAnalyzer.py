import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import base64
from datetime import datetime, date, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
from ticker_mappings import COMPANY_TICKER_MAPPINGS
import requests
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import re
from datetime import time as time_class  # Rename the import to avoid conflict

class SheetManager:
    def __init__(self):
        try:
            self.client = self._authenticate()
            self.connected = True
        except Exception as e:
            print(f"Warning: Could not connect to Google Sheets ({str(e)}). Using local mode.")
            self.connected = False
            self.local_records = {}
        
        self.FIELD_NAMES = [
            'date', 'calculation_time', 
            'calculated_nav', 'official_nav', 
            'difference', 'percentage_diff',
            'fund_name', 'equity_portion'
        ]
        
    def get_sheet_for_fund(self, fund_name):
        """Get or create a worksheet for the specific fund"""
        if not self.connected:
            if fund_name not in self.local_records:
                self.local_records[fund_name] = []
            return None  # Return None for local mode
        
        try:
            # Open the main spreadsheet
            spreadsheet = self.client.open("NAV Results")
        except gspread.SpreadsheetNotFound:
            # Create new spreadsheet if it doesn't exist
            spreadsheet = self.client.create("NAV Results")
        
        try:
            # Try to get the worksheet for this fund
            worksheet = spreadsheet.worksheet(fund_name)
        except gspread.WorksheetNotFound:
            # Create new worksheet if it doesn't exist
            worksheet = spreadsheet.add_worksheet(title=fund_name, rows=1000, cols=20)
            worksheet.append_row(self.FIELD_NAMES)
        
        return worksheet

    def get_todays_record(self, fund_name, today_date):
        """Get today's existing record if it exists"""
        worksheet = self.get_sheet_for_fund(fund_name)
        
        if not self.connected:
            # Local mode handling
            for record in self.local_records.get(fund_name, []):
                if record['date'] == today_date:
                    return record
            return None
        
        records = worksheet.get_all_records()
        for idx, record in enumerate(records, start=2):  # Rows start at 2
            if record['date'] == today_date:
                record['row_num'] = idx  # Store the row number for updates
                return record
        return None

    def update_record(self, fund_name, row_num, record_data):
        """Update an existing record"""
        worksheet = self.get_sheet_for_fund(fund_name)
        
        if not self.connected:
            # Local mode handling
            for i, record in enumerate(self.local_records.get(fund_name, [])):
                if record['date'] == record_data['date']:
                    self.local_records[fund_name][i] = record_data
                    break
            return
        
        row_data = [record_data.get(field, '') for field in self.FIELD_NAMES]
        worksheet.update(f"A{row_num}:H{row_num}", [row_data])

    def _authenticate(self):
        """Authenticate with Google Sheets"""
        if 'GDRIVE_CREDENTIALS' not in os.environ:
            raise Exception("GDRIVE_CREDENTIALS environment variable not set")
            
        creds_json = base64.b64decode(os.environ['GDRIVE_CREDENTIALS']).decode('utf-8')
        creds_dict = json.loads(creds_json)
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    
    def get_all_records(self, fund_name):
        """Get all records from the fund's sheet as dictionaries"""
        worksheet = self.get_sheet_for_fund(fund_name)
        
        if not self.connected:
            return self.local_records.get(fund_name, [])
        
        return worksheet.get_all_records()
    
    def add_record(self, fund_name, record_data):
        """Add a new record to the fund's sheet"""
        worksheet = self.get_sheet_for_fund(fund_name)
        
        if not self.connected:
            if fund_name not in self.local_records:
                self.local_records[fund_name] = []
            self.local_records[fund_name].append(record_data)
            return
        
        row_data = [record_data.get(field, '') for field in self.FIELD_NAMES]
        worksheet.append_row(row_data)
    
    def update_official_nav(self, fund_name, official_nav, target_date=None):
        """Update the official NAV for a specific fund and date"""
        if target_date is None:
            target_date = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")
        
        worksheet = self.get_sheet_for_fund(fund_name)
        
        if not self.connected:
            updated = False
            for record in self.local_records.get(fund_name, []):
                if record['date'] == target_date:
                    if not record['official_nav'] or record['official_nav'] == '':
                        record['official_nav'] = str(official_nav)
                        
                        if record['calculated_nav'] and record['calculated_nav'] != '':
                            calculated = float(record['calculated_nav'])
                            diff = official_nav - calculated
                            percentage_diff = (diff / calculated) * 100
                            
                            record['difference'] = str(round(diff, 4))
                            record['percentage_diff'] = str(round(percentage_diff, 4))
                        
                        updated = True
            return updated
        
        records = worksheet.get_all_records()
        updated = False
        
        for idx, row in enumerate(records, start=2):  # Start from row 2 (1-based index)
            if row['date'] == target_date:
                if not row['official_nav'] or row['official_nav'] == '':
                    # Update official NAV
                    worksheet.update_cell(idx, 4, str(official_nav))
                    
                    # Calculate and update difference if calculated_nav exists
                    if row['calculated_nav'] and row['calculated_nav'] != '':
                        calculated = float(row['calculated_nav'])
                        diff = official_nav - calculated
                        percentage_diff = (diff / calculated) * 100
                        
                        worksheet.update_cell(idx, 5, str(round(diff, 4)))
                        worksheet.update_cell(idx, 6, str(round(percentage_diff, 4)))
                    
                    updated = True
                    break
        
        return updated
    
    def get_previous_calculation(self, fund_name):
        """Get yesterday's calculation for a fund"""
        yesterday = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")
        records = self.get_all_records(fund_name)
        
        for row in reversed(records):
            if row['date'] == yesterday:
                return {
                    'calculated_nav': float(row['calculated_nav']) if row['calculated_nav'] else None,
                    'official_nav': float(row['official_nav']) if row['official_nav'] else None
                }
        return None
    
    def show_comparison(self, fund_name):
        """Show historical comparison for a fund"""
        print(f"\nHistorical Comparison for {fund_name}:")
        print("{:<12} {:<10} {:<12} {:<12} {:<10} {:<8}".format(
            'Date', 'Calc NAV', 'Official NAV', 'Difference', '% Diff', 'Time'
        ))
        print("-" * 70)
        
        records = self.get_all_records(fund_name)
        found_data = False

        count = 0
        for row in reversed(records):
            date_str = row['date']
            calc_nav = row['calculated_nav'] or '-'
            official_nav = row['official_nav'] or '-'
            diff = row['difference'] or '-'
            perc_diff = row['percentage_diff'] or '-'
            time_str = row['calculation_time'] or '-'
            
            print("{:<12} {:<10} {:<12} {:<12} {:<10} {:<8}".format(
                date_str, calc_nav, official_nav, diff, perc_diff, time_str
            ))
            found_data = True
            
            if count == 2:
                break
            count = count + 1
        
        if not found_data:
            print(f"No historical data found for {fund_name}")

class MutualFundAnalyzer:
    def __init__(self, url, equity_portion=None, max_workers=None, base_workers=5):
        self.url = url
        self.equity_portion = equity_portion
        self.fund_name = self._extract_fund_name()
        self.base_workers = base_workers
        self.max_workers = max_workers
        self.dynamic_workers = None
        self.sheet_manager = SheetManager()
        
        self.companies_ticker_and_Exchange = COMPANY_TICKER_MAPPINGS
        self.Last_day_closed = None
        self.stock_search_company_name_char_str = []
        self.stock_search_company_name_stock_correcponding_holding_pairs = {}
        self.companies_ticker_and_Exchange_of_this_particular_MF = {}

    def _calculate_optimal_workers(self, holdings_count):
        """
        Calculate optimal number of workers based on holdings count
        Uses square root scaling with base minimum and optional maximum
        """
        # Square root scaling gives us a good balance between parallelism and overhead
        calculated = max(self.base_workers, math.floor(math.sqrt(holdings_count) * 1.8))
        
        # Apply maximum limit if specified
        if self.max_workers is not None:
            return min(calculated, self.max_workers)
        return calculated

    def _extract_fund_name(self):
        """Extract fund name from URL"""
        # Extract the fund name part from the URL
        fund_part = self.url.split('/')[-1].replace('-direct-growth', '').replace('-', ' ').title()
        # Remove any "Direct" or "Growth" suffixes
        fund_part = fund_part.replace('Direct', '').replace('Growth', '').strip()
        return fund_part

    def _clean_company_name(self, name):
        """Clean and standardize company names"""
        return (name.upper()
                .replace("&", "AND")
                .replace("LTD.", "LTD")
                .replace("LIMITED", "LTD")
                .replace(" ", "")
                .replace("(", "")
                .replace(")", ""))

    def _get_equity_percentage_parallel(self):
        """Fetch equity percentage using Selenium in parallel with other operations"""
        def fetch_equity():
            try:
                chrome_options = Options()
                chrome_options.add_argument("--headless=new")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--window-size=1920,1080")

                driver = webdriver.Chrome(
                    service=ChromeService(ChromeDriverManager().install()),
                    options=chrome_options
                )
                driver.get(self.url)

                time.sleep(2)

                # Scroll to bottom
                last_height = driver.execute_script("return document.body.scrollHeight")
                while True:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                body_text = driver.find_element(By.TAG_NAME, "body").text

                pattern = r"Equity\s*\n\s*([+-]?[0-9]*\.?[0-9]+%)"
                match = re.search(pattern, body_text)

                if match:
                    equity_str = match.group(1).replace('%', '')
                    return float(equity_str) / 100
                else:
                    return 0.95
                return None

            except Exception as e:
                print(f"Error fetching equity percentage: {str(e)}")
                return None
            finally:
                driver.quit()

        # Start equity fetching in parallel
        with ThreadPoolExecutor(max_workers=1) as executor:
            equity_future = executor.submit(fetch_equity)
            
            # Proceed with other operations while equity fetches
            if not self._fetch_mf_data_without_equity():
                return False

            # Get equity result
            self.equity_portion = equity_future.result()
            if self.equity_portion is None:
                print("Warning: Could not determine equity portion, defaulting to 95.4%")
                self.equity_portion = 0.954
            else:
                print(f"Automatically determined equity portion: {self.equity_portion*100:.1f}%")

        return True

    def _fetch_mf_data_without_equity(self):
        """Fetch MF data without equity percentage"""
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Get last day NAV
            script_content = soup.find('script', {'id': "__NEXT_DATA__"})
            if not script_content:
                raise ValueError("Holdings data script not found")
            script_content = script_content.text

            nav_element_start = script_content.find('"nav":')
            nav_element = script_content[nav_element_start+6:nav_element_start+13]
        
            if not nav_element:
                raise ValueError("NAV element not found")

            self.Last_day_closed = float(nav_element.strip().replace(",",""))
            
            # Get holdings data
            holdings_start = script_content.find('"holdings":')
            holdings_end = script_content.find('"nav":')

            if holdings_start == -1 or holdings_end == -1:
                raise ValueError("Could not parse holdings data")

            holdings_text = script_content[holdings_start+11:holdings_end-1].replace('null', 'None')
            holdings_list = eval(holdings_text)

            # Process holdings data
            self.stock_search_company_name_char_str = [
                self._clean_company_name(d['company_name'])
                for d in holdings_list
            ]

            self.stock_search_company_name_stock_correcponding_holding_pairs = {
                company: d['corpus_per']
                for company, d in zip(self.stock_search_company_name_char_str, holdings_list)
            }

            # Calculate optimal workers based on holdings count
            holdings_count = len(self.stock_search_company_name_char_str)
            self.dynamic_workers = self._calculate_optimal_workers(holdings_count)
            print(f"\nDetected {holdings_count} holdings - using {self.dynamic_workers} parallel workers")

            # Map company names to tickers
            for company in self.stock_search_company_name_char_str:
                if company in self.companies_ticker_and_Exchange:
                    self.companies_ticker_and_Exchange_of_this_particular_MF[company] = \
                        self.companies_ticker_and_Exchange[company]
                else:
                    print(f"Warning: No ticker mapping found for {company}")
                    self.companies_ticker_and_Exchange_of_this_particular_MF[company] = ['UNKNOWN', '0']

            return True

        except Exception as e:
            print(f"Error fetching MF data: {str(e)}")
            return False

    def fetch_mf_data(self):
        """Main method to fetch all data with parallel equity fetching"""
        if self.equity_portion is None:
            return self._get_equity_percentage_parallel()
        else:
            return self._fetch_mf_data_without_equity()

    def _get_price(self, ticker, exchange, price_type="current"):
        """Unified method to get stock prices"""
        try:
            url = f'https://www.google.com/finance/quote/{ticker}:{exchange}?hl=en'
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            class_name = "YMlKec fxKbKc" if price_type == "current" else "P6K39c"
            price_element = soup.find(class_=class_name)

            if price_element:
                return float(price_element.text.strip()[1:].replace(",", ""))
        except Exception as e:
            print(f"Error getting {price_type} price for {ticker} on {exchange}: {str(e)}")
        return None

    def _fetch_company_prices(self, company):
        """Helper method for parallel execution"""
        ticker_and_exchange = self.companies_ticker_and_Exchange_of_this_particular_MF[company]
        holding_pct = self.stock_search_company_name_stock_correcponding_holding_pairs[company]
        
        if ticker_and_exchange[0] == 'UNKNOWN' and ticker_and_exchange[1] == '0':
            return company, (0, 0)
            
        # Try NSE first
        if ticker_and_exchange[0] != 'UNKNOWN':
            current_price = self._get_price(ticker_and_exchange[0], "NSE", "current")
            last_price = self._get_price(ticker_and_exchange[0], "NSE", "last")
            if current_price is not None and last_price is not None:
                return company, (
                    (current_price * holding_pct) / 100,
                    (last_price * holding_pct) / 100
                )
        
        # Fallback to BSE
        if ticker_and_exchange[1] != '0':
            current_price = self._get_price(ticker_and_exchange[1], "BOM", "current")
            last_price = self._get_price(ticker_and_exchange[1], "BOM", "last")
            if current_price is not None and last_price is not None:
                return company, (
                    (current_price * holding_pct) / 100,
                    (last_price * holding_pct) / 100
                )
        
        print(f"Skipping company with tickers: {ticker_and_exchange}")
        return company, (0, 0)

    def calculate_current_status(self):
        """Calculate current MF status with dynamic parallel workers"""
        cummulative_current = 0
        cummulative_last = 0

        with ThreadPoolExecutor(max_workers=self.dynamic_workers) as executor:
            futures = {
                executor.submit(self._fetch_company_prices, company): company
                for company in self.stock_search_company_name_char_str
            }
            
            for future in as_completed(futures):
                company, (current, last) = future.result()
                cummulative_current += current
                cummulative_last += last

        if cummulative_last == 0:
            return 0

        return ((cummulative_current - cummulative_last) / cummulative_last) * 100

    def run_analysis(self, iterations=2):
        """Run the analysis with tracking and comparison"""
        print(f"\nAnalyzing: {self.fund_name}")

        # Time thresholds
        market_open = time_class(9, 15)
        market_close = time_class(15, 30)
        current_time = datetime.now().time()
        today = date.today().strftime("%d/%m/%Y")

        # 1. First handle yesterday's official NAV update
        prev_calc = self.sheet_manager.get_previous_calculation(self.fund_name)
        if prev_calc and (prev_calc['official_nav'] is None or prev_calc['official_nav'] == ''):
            official_nav = self.fetch_official_nav()
            if official_nav:
                self.sheet_manager.update_official_nav(self.fund_name, official_nav)
                print(f"Updated yesterday's official NAV to {official_nav}")

        # 2. Fetch current data and calculate NAV
        if not self.fetch_mf_data():
            print("Failed to fetch mutual fund data")
            return

        percent_change = self.calculate_current_status()
        if percent_change is None:
            print("Error: Could not calculate percentage change")
            return

        print(f"\nLast Day Closed NAV: {self.Last_day_closed}")

        equity_adjusted_nav = self.Last_day_closed * (1 + (percent_change * self.equity_portion) / 100)
        rounded_nav = round(equity_adjusted_nav, 4)

        print("\nCalculation Results:")
        print(f"Current NAV (equity-adjusted): {rounded_nav:.4f}")
        print(f"Calculation Time: {current_time}")

        # 3. Determine if we should store this calculation
        should_store = False
        existing_today = self.sheet_manager.get_todays_record(self.fund_name, today)

        if current_time < market_open:
            print("Before market open - not storing data")
        elif current_time <= market_close:
            # Market hours - only store if different from existing
            if not existing_today:
                should_store = True
                reason = "First calculation of the day"
            else:
                # Compare NAV values
                existing_nav = float(existing_today['calculated_nav'])
                if abs(existing_nav - rounded_nav) > 0.0001:
                    should_store = True
                    reason = "Significant NAV change"
                else:
                    print("NAV unchanged - not storing update")
        else:
            # After market close - store only if no closing record exists
            if not existing_today:
                should_store = True
                reason = "Market close recording"
            else:
                # Parse the existing time string to compare
                existing_time_str = existing_today['calculation_time']
                try:
                    existing_time = datetime.strptime(existing_time_str, "%H:%M:%S").time()
                    if existing_time < market_close:
                        should_store = True
                        reason = "Market close recording"
                    else:
                        print("Already have closing NAV - not storing")
                except ValueError:
                    print("Could not parse existing time - storing new record")
                    should_store = True
                    reason = "Invalid existing time format"

        # 4. Store the record if needed
        if should_store:
            new_record = {
                'date': today,
                'calculation_time': current_time.strftime("%H:%M:%S"),
                'calculated_nav': rounded_nav,
                'official_nav': '',
                'difference': '',
                'percentage_diff': '',
                'fund_name': self.fund_name,
                'equity_portion': self.equity_portion
            }
            try:
                if existing_today:
                    # Update existing record
                    self.sheet_manager.update_record(self.fund_name, existing_today['row_num'], new_record)
                    print(f"✓ Updated today's record ({reason})")
                else:
                    # Add new record
                    self.sheet_manager.add_record(self.fund_name, new_record)
                    print(f"✓ Added new record ({reason})")
            except Exception as e:
                print(f"Failed to update sheet: {str(e)}")

        # 5. Show historical comparison
        self.sheet_manager.show_comparison(self.fund_name)

    def fetch_official_nav(self):
        """Fetch the official NAV from Groww"""
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            script_content = soup.find('script', {'id': "__NEXT_DATA__"})
            script_content = script_content.text
            nav_element_start = script_content.find('"nav":')
            nav_element = script_content[nav_element_start+6:nav_element_start+13]

            if nav_element:
                return float(nav_element.strip())
        except Exception as e:
            print(f"Error fetching official NAV: {str(e)}")
        return None

# if __name__ == "__main__":
#     urls = [
#         'https://groww.in/mutual-funds/sbi-psu-fund-direct-growth',
#         'https://groww.in/mutual-funds/icici-prudential-value-discovery-fund-direct-growth',
#         # Add more URLs as needed
#     ]

#     for url in urls:
#         analyzer = MutualFundAnalyzer(url, base_workers=5)
#         analyzer.run_analysis()


