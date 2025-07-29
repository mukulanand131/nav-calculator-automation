import csv
import os
from datetime import datetime, date
from os import replace
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
from datetime import timedelta  # Add at top of file


class NAVTracker:
    CSV_FILE = "nav_comparison.csv"
    FIELD_NAMES = [
        'date', 'calculation_time', 
        'calculated_nav', 'official_nav', 
        'difference', 'percentage_diff',
        'fund_name', 'equity_portion'
    ]
    
    def __init__(self):
        self.ensure_csv_header()
    
    def ensure_csv_header(self):
        """Ensure CSV file exists with proper headers"""
        try:
            if not os.path.exists(self.CSV_FILE):
                with open(self.CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=self.FIELD_NAMES)
                    writer.writeheader()
        except Exception as e:
            print(f"Error ensuring CSV header: {str(e)}")
            raise
    
    def save_calculation(self, fund_name, calculated_nav, equity_portion):
        """Save today's calculation to CSV with robust error handling"""
        try:
            # Use absolute path for reliability
            csv_path = os.path.abspath(self.CSV_FILE)
            today = date.today().strftime("%d/%m/%Y")
            now = datetime.now()
            now_time_str = now.strftime("%H:%M:%S")
            new_nav_rounded = round(calculated_nav, 4)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            
            # Read existing data
            existing_rows = []
            if os.path.exists(csv_path):
                with open(csv_path, mode='r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    existing_rows = list(reader)
            
            # Check for duplicates after 3:30 PM (timezone-aware)
            cutoff_time = time(15, 30)
            current_time = now.time()
            
            if current_time >= cutoff_time:
                for row in reversed(existing_rows):
                    try:
                        if (row['date'] == today and 
                            row['fund_name'] == fund_name and
                            float(row['calculated_nav']) == new_nav_rounded):
                            
                            row_time = datetime.strptime(
                                row['calculation_time'], 
                                "%H:%M:%S"
                            ).time()
                            
                            if row_time >= cutoff_time:
                                print("Duplicate entry found after 3:30 PM. Skipping save.")
                                return
                    except (ValueError, KeyError):
                        continue
            
            # Prepare new data
            new_data = {
                'date': today,
                'calculation_time': now_time_str,
                'calculated_nav': new_nav_rounded,
                'official_nav': None,
                'difference': None,
                'percentage_diff': None,
                'fund_name': fund_name,
                'equity_portion': equity_portion
            }
            
            # Write to CSV
            file_exists = os.path.exists(csv_path)
            with open(csv_path, mode='a' if file_exists else 'w', 
                     newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.FIELD_NAMES)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(new_data)
            
            print(f"Saved calculation to {csv_path}")
            
        except Exception as e:
            print(f"Error saving calculation: {str(e)}")
            raise
    
    def get_previous_calculation(self, fund_name):
        """Get yesterday's calculation with improved error handling"""
        try:
            yesterday = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")
            
            if not os.path.exists(self.CSV_FILE):
                return None
                
            with open(self.CSV_FILE, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reversed(list(reader)):
                    if row['date'] == yesterday and row['fund_name'] == fund_name:
                        try:
                            return {
                                'calculated_nav': float(row['calculated_nav']),
                                'official_nav': (
                                    None if not row['official_nav'] 
                                    else float(row['official_nav'])
                                )
                            }
                        except (ValueError, TypeError):
                            continue
            return None
        except Exception as e:
            print(f"Error getting previous calculation: {str(e)}")
            return None
    
    def update_official_nav(self, fund_name, official_nav):
        """Update yesterday's official NAV with transaction safety"""
        try:
            yesterday = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")
            updated = False
            
            if not os.path.exists(self.CSV_FILE):
                return
                
            # Read all data
            rows = []
            with open(self.CSV_FILE, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
            
            # Update records
            for row in reversed(rows):
                if row['date'] == yesterday and row['fund_name'] == fund_name:
                    if not row['official_nav']:
                        row['official_nav'] = official_nav
                        if row['calculated_nav']:
                            try:
                                calculated = float(row['calculated_nav'])
                                diff = official_nav - calculated
                                row['difference'] = round(diff, 4)
                                row['percentage_diff'] = round(
                                    (diff / calculated) * 100, 4
                                )
                            except (ValueError, TypeError):
                                pass
                        updated = True
            
            # Write back if updated
            if updated:
                # Write to temporary file first
                temp_file = self.CSV_FILE + '.tmp'
                with open(temp_file, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=self.FIELD_NAMES)
                    writer.writeheader()
                    writer.writerows(rows)
                
                # Replace original file
                os.replace(temp_file, self.CSV_FILE)
                
                print(f"Updated official NAV for {yesterday}")
        except Exception as e:
            print(f"Error updating official NAV: {str(e)}")
            raise
    
    def show_comparison(self, fund_name):
        """Show historical comparison with improved formatting"""
        try:
            if not os.path.exists(self.CSV_FILE):
                print("No historical data available")
                return
                
            print("\nHistorical Comparison:")
            print("{:<12} {:<12} {:<12} {:<12} {:<10} {:<8}".format(
                'Date', 'Calc NAV', 'Official NAV', 'Difference', '% Diff', 'Time'
            ))
            print("-" * 70)
            
            with open(self.CSV_FILE, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                records = [row for row in reader if row['fund_name'] == fund_name]
                
                for row in reversed(records[-10:]):  # Show last 10 records
                    try:
                        date_str = row['date']
                        calc_nav = row['calculated_nav'] or '-'
                        official_nav = row['official_nav'] or '-'
                        diff = row['difference'] or '-'
                        perc_diff = row['percentage_diff'] or '-'
                        time_str = row['calculation_time'] or '-'
                        
                        print("{:<12} {:<12} {:<12} {:<12} {:<10} {:<8}".format(
                            date_str, 
                            calc_nav, 
                            official_nav, 
                            diff, 
                            f"{perc_diff}%" if perc_diff != '-' else perc_diff,
                            time_str
                        ))
                    except KeyError:
                        continue
        except Exception as e:
            print(f"Error showing comparison: {str(e)}")

class MutualFundAnalyzer:
    def __init__(self, url, equity_portion=None, max_workers=None, base_workers=5):
        self.url = url
        self.equity_portion = equity_portion
        self.fund_name = self._extract_fund_name()
        self.base_workers = base_workers
        self.max_workers = max_workers
        self.dynamic_workers = None
        self.tracker = NAVTracker()
        
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
        return (self.url[30:]).title()

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
            nav_element = soup.find(class_="fd12Cell contentPrimary bodyLargeHeavy")
            if not nav_element:
                raise ValueError("NAV element not found")

            self.Last_day_closed = float(nav_element.text.strip()[1:].replace(",", ""))

            # Get holdings data
            script_content = soup.find('script', {'id': "__NEXT_DATA__"})
            if not script_content:
                raise ValueError("Holdings data script not found")

            script_content = script_content.text
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

        # Check if we have previous calculation to compare with
        prev_calc = self.tracker.get_previous_calculation(self.fund_name)
        if prev_calc and prev_calc['official_nav'] is None:
            # We have yesterday's calculation but no official NAV yet
            # Fetch today's official NAV (which is yesterday's closing)
            official_nav = self.fetch_official_nav()
            if official_nav:
                self.tracker.update_official_nav(self.fund_name, official_nav)
        
        start_total_time = datetime.now()
        
        if not self.fetch_mf_data():
            print("Failed to fetch mutual fund data. Please check the URL and try again.")
            return

        fetch_time = (datetime.now() - start_total_time).total_seconds()
        print(f"\nData fetching completed in {fetch_time:.2f} seconds")
        print(f"Last day NAV: {self.Last_day_closed:.2f}")

        # Calculate current NAV
        print("\nCalculating current NAV...")
        percent_change = self.calculate_current_status()
        current_nav = self.Last_day_closed * (1 + percent_change / 100)
        equity_adjusted_nav = self.Last_day_closed * (1 + (percent_change * self.equity_portion) / 100)

        print("\nCalculation Results:")
        print(f"Percentage change (full): {percent_change:.2f}%")
        print(f"Current NAV (full): {current_nav:.2f}")
        print(f"Equity-adjusted change: {percent_change * self.equity_portion:.2f}%")
        print(f"Current NAV (equity-adjusted): {equity_adjusted_nav:.2f}")

        # Save today's calculation
        current_time = datetime.now().time()
        if current_time >= datetime.strptime("15:30:00", "%H:%M:%S").time():
            self.tracker.save_calculation(self.fund_name, equity_adjusted_nav, self.equity_portion)
        
        # Show historical comparison
        self.tracker.show_comparison(self.fund_name)

    def fetch_official_nav(self):
        """Fetch the official NAV from Groww"""
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            nav_element = soup.find(class_="fd12Cell contentPrimary bodyLargeHeavy")
            if nav_element:
                return float(nav_element.text.strip()[1:].replace(",", ""))
        except Exception as e:
            print(f"Error fetching official NAV: {str(e)}")
        return None

if __name__ == "__main__":
    
    
    urls = [
        'https://groww.in/mutual-funds/sbi-psu-fund-direct-growth',
        # Add more URLs as needed
    ]

    for url in urls:
        analyzer = MutualFundAnalyzer(url, base_workers=5)
        analyzer.run_analysis()
