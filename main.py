# from MutualFundAnalyzer import MutualFundAnalyzer

# if __name__ == "__main__":
#     analyzer = MutualFundAnalyzer("https://groww.in/mutual-funds/sbi-psu-fund-direct-growth")
#     analyzer.run_analysis()


import os
from datetime import datetime, timedelta
import pytz
from MutualFundAnalyzer import MutualFundAnalyzer

def main():
    try:
        # Set timezone for the entire application
        ist = pytz.timezone('Asia/Kolkata')
        os.environ['TZ'] = 'Asia/Kolkata'
        
        print(f"\n{'='*40}")
        print(f"Analysis started at: {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
        print(f"{'='*40}\n")

        # Initialize analyzer with URL
        fund_url = "https://groww.in/mutual-funds/sbi-psu-fund-direct-growth"
        analyzer = MutualFundAnalyzer(fund_url)
        
        # Run analysis
        analyzer.run_analysis()

        # Verify CSV file was created properly
        csv_path = os.path.abspath("nav_comparison.csv")
        if os.path.exists(csv_path):
            print(f"\nCSV file created at: {csv_path}")
            print(f"File size: {os.path.getsize(csv_path)} bytes")
        else:
            print("\nWarning: CSV file was not created")

    except Exception as e:
        print(f"\n❌ Error in main execution: {str(e)}")
        raise
    finally:
        print(f"\n{'='*40}")
        print(f"Analysis completed at: {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
        print(f"{'='*40}")

if __name__ == "__main__":
    main()
