from MutualFundAnalyzer import MutualFundAnalyzer

if __name__ == "__main__":
    urls = [
        'https://groww.in/mutual-funds/sbi-psu-fund-direct-growth',
        'https://groww.in/mutual-funds/icici-prudential-value-discovery-fund-direct-growth',
        # Add more URLs as needed
    ]

    for url in urls:
        analyzer = MutualFundAnalyzer(url, base_workers=5)
        analyzer.run_analysis()
