#!/bin/bash

cd /home/ec2-user/stock-history/
. /home/ec2-user/.local/share/virtualenvs/stock-history-jteLTlvN/bin/activate 
python3 /home/ec2-user/stock-history/historical_scraping_no_filters_sushil.py --tickers /home/ec2-user/stock-history/inputs/keywords_stocks.csv --dates /home/ec2-user/stock-history/inputs/one_date.csv --output_location /home/ec2-user/stock-history/output/

