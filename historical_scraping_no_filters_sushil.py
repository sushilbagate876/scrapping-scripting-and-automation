import sys
import datetime
import requests
from bs4 import BeautifulSoup
from newspaper import Article
from random import randint
import time
from fake_useragent import UserAgent
import regex as re
from langdetect import detect
import os
import csv
from pathlib import Path
import threading
import secrets
import urllib3
import json
from datetime import timedelta
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NewsScraper:

    def __init__(self, proxies, date_file, no_news_tickers_file):
        """docstring for __init__"""
        self.proxies = proxies
        self.date_file = date_file
        self.no_news_tickers_file = no_news_tickers_file
        self.no_news_header = ['ticker', 'date_start', 'date_end', 'country']
        
    
    def get_article(self, ticker, description, country, language, output_location):

        with open(self.date_file) as csv_dates:
            dates = csv.DictReader(csv_dates)
            for date in dates:
                print("Ticker: {} - Country: {} - Date start: {} -- end: {}".format(ticker, country, date['date_start'], date['date_end']))
                self.search(ticker, description, date, country, language, output_location)


    def search(self, ticker, description, date, country, language, output_location):
        """docstring for search"""

        scrape_date = datetime.datetime.now().strftime("%Y%m%d")
        scrape_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        scrape_date_time = scrape_date + scrape_time
        scrape_data_list = []
        new_date = datetime.datetime.strptime(date['date_start'], "%m/%d/%Y")
        file_path = '{}/{}/{}/{}/'.format(output_location,new_date.strftime("%Y"), new_date.strftime("%b"), new_date.strftime("%d"))
        file_name = "{}/{}_{}.txt".format(file_path, language, description)

        if Path(file_name).is_file():
            if Path(file_name).stat().st_size > 0:
                print("\tFile available -- skip")
                return

        path = Path(file_path)
        path.mkdir(parents=True, exist_ok=True)
        ua = UserAgent()
        start_date = date['date_start']
        end_date = date['date_end']
        search_query = "https://www.google.com/search?&q=" + ticker +\
                       "&tbs=cdr:1,cd_min:" + start_date + ",cd_max:" + end_date + \
                       "&tbo=1&source=lnms&tbm=nws&cad=h&hl=" + language + "&cr="+country

        headers = {'User-Agent': ua.chrome, 'Accept-Language': 'en-US,en;q=0.5', 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}

        content = None
        try_again = 0
        #exit after 50 retry
        max_retry = 5
        while content is None:
            rand_session = secrets.token_hex(10)
            new_proxy = {'https': self.proxies['https'].format(rand_session), 'http': self.proxies['http'].format(rand_session)}
            print("\tTry again, trial number = {}".format(try_again))
            result = requests.get(search_query, headers=headers, proxies=new_proxy, verify=False)
            print(result.status_code)

            if result.status_code == 200:
                try:
                    content = result.text
                    soup = BeautifulSoup(content, "lxml")
                    p = soup.find('p', attrs={'role': 'heading'})
                    if p is None:
                        p = soup.find('div', attrs={'role': 'heading'})
                    if p:
                        if re.search(r'.*did not', p.get_text(), re.I|re.M|re.S):
                            print("\t----NO NEWS---- {} --- {}".format(ticker, start_date, end_date))
                            return
                    if soup.find('body', attrs={'id': 'gsr'}) is None:
                        try_again += 1
                        if try_again <= max_retry:
                            content = None
                            time.sleep(randint(10, 30))
                            continue
                    search_news_article = soup.find_all("div", attrs={'id': 'search'})
                    link_list = []

                    for a in search_news_article:
                        try:
                            all_link = a.find_all("a")
                            for link in all_link:
                                data_link = link.get('href')
                                link_list.append(data_link)
                        except:
                            pass

                    link_set = set(link_list)
                    for data_link in link_set:
                        partial_news = ''
                        full_news = ''
                        try:
                            article = Article(data_link, language=language)
                            article.download()
                            article.parse()
                            partial_news = article.title
                            full_news = article.text.replace('\n', '')
                        except Exception as e:
                            pass

                        # Create a dictionary of single news article with all details.
                        data_dict = {"Scrape Date & Time": scrape_date_time,
                                     "Search_Start_Date": start_date,
                                     "Search_End_Date": end_date,
                                     "Language": language,
                                     "Country" : country,
                                     "Symbol": '',
                                     "Full_Name": '',
                                     "ISIN": description,
                                     "Ticker": ticker,
                                     "Description": description,
                                     "News Link": data_link,
                                     "Partial News Article": partial_news,
                                     "Full News Article": full_news
                                     }
                        scrape_data_list.append(data_dict)
                except Exception as e:
                    print(e)
            if len(scrape_data_list) <= 0:
                content = None

            try_again += 1
            if content is None and try_again <= max_retry:
                time.sleep(randint(10, 30))
                continue

            if try_again > max_retry:
                break
        if len(scrape_data_list) > 0:
            self.save_data(file_path, scrape_data_list, ticker, description, language)
        else:
            print("---- Could not find any news : Len(scrape_data_list) = 0! -----")
            with open(self.no_news_tickers_file, 'a') as csv_no_news:
                info = {"ticker": ticker, "date_start": start_date, "date_end": end_date}
                writer = csv.DictWriter(csv_no_news, fieldnames=self.no_news_header)
                writer.writerow(info)

        return True


    def save_data(self, file_path, scrape_data_list, ticker, description, language):
        file_name = "{}/{}_{}.txt".format(file_path, language, description)

        # Save json format
        json_file_name = file_name
        output_json = scrape_data_list
        with open(json_file_name,'w') as f:
            json.dump(output_json,f,indent=4,ensure_ascii=False)

        return

# Get Date and Time of scraping.
today = datetime.datetime.now().strftime("%m/%d/%Y")
yesterday = (datetime.datetime.now()  - timedelta(days=1)).strftime("%m/%d/%Y")
df = pd.DataFrame({'date_start':[yesterday],'date_end':[today]})
df.to_csv(r'/home/ec2-user/stock-history/inputs/one_date.csv')

#proxy settings
proxy_url_http = 'lum-customer-hl_37958623-zone-zone1-unblocker-country-us-session-{}:mgpl12i86hcu@zproxy.lum-superproxy.io:22225'
lum_proxy = {'http': proxy_url_http, 'https': proxy_url_http}

#max number of thread
max_thread = 100

date_file = None
ticker_file = None
output_location = None
no_news_tickers_file = None
missing = False

args_list = dict()
arg_count = 0
for arg in sys.argv:
    arg_count += 1
    if re.search('^--', arg):
        try:
            args_list[arg] = sys.argv[arg_count]
        except Exception as e:
            pass
if '--tickers' in args_list:
    ticker_file = args_list['--tickers']

if '--output_location' in args_list:
    output_location = args_list['--output_location']

if '--dates' in args_list:
    date_file = args_list['--dates']

if '--thread' in args_list:
    max_thread = args_list['--thread']
    try:
        max_thread = int(max_thread)
    except Exception as e:
        print(e)

if '--missing' in args_list:
    missing = True
    no_news_tickers_file = args_list['--missing']

if missing is False:
    if (date_file is None) or (ticker_file is None) or (output_location is None):
        print("\nError!!!")
        print("Date file, ticker file or country are missing.\n\nEg: python historical_scraping_daly_new.py --tickers inputs/stock_names.csv --dates inputs/dates_for_historical_download.csv --output_location output_folder\n")
        exit()

if missing:
    print("------Process missing ticker and dates.----------")
    print("------FILE: {}--------".format(no_news_tickers_file))
    if no_news_tickers_file is None:
        print("Input file does not exist")
    else:
        no_news_tickers_file_new = 'inputs/missing/no_news_tickers_{}.csv'.format(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
        news_scraper_obj = NewsScraper(lum_proxy, date_file, no_news_tickers_file_new)

        with open(no_news_tickers_file_new, 'w') as csv_missing:
            fields = ["ticker", "date_start", "date_end", "country"]
            writer = csv.DictWriter(csv_missing, fieldnames=fields)
            writer.writeheader()

        with open(no_news_tickers_file) as csv_missing:
            rows = csv.DictReader(csv_missing)
            for row in rows:
                ticker = row['Keyword']
                date = {"date_start": row['date_start'], "date_end": row['date_end']}
                country = row['Country']
                language = row['Language']
                description = row['ISIN']
                
                print("Ticker: {} - Date start: {} -- end: {}".format(ticker, date['date_start'], date['date_end']))
                news_scraper_obj.search(ticker, description, date, country, language, output_location)
else:
    no_news_tickers_file = 'inputs/missing/no_news_tickers_{}.csv'.format(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    with open(no_news_tickers_file, 'w') as csv_missing:
        fields = ["ticker", "date_start", "date_end"]
        writer = csv.DictWriter(csv_missing, fieldnames=fields)
        writer.writeheader()

    with open(ticker_file) as csv_ticker:
        rows = csv.DictReader(csv_ticker)
        news_scraper_obj = NewsScraper(lum_proxy, date_file, no_news_tickers_file)
        threads = [threading.Thread(target=news_scraper_obj.get_article, args=[t['Keyword'], t['ISIN'], t['Country'], t['Language'],output_location]) for t in rows]
        for thread in threads:
            thread.start()
            while threading.active_count() >= max_thread:
                print("\tTotal process: {}".format(threading.active_count()))
                time.sleep(2)


