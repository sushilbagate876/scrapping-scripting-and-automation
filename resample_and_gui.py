import pandas as pd
import numpy as np
import sys
import re
from langdetect import detect
import os 
import investpy
import pickle
from os import listdir
from os.path import isfile, join
from tqdm import tqdm
import datetime as dt
from nltk.tokenize import treebank
import nltk
import csv
import threading
import time
from datetime import timedelta
from io import StringIO

country_code_for_investpy = 'Thailand'

df = pd.read_csv('/home/ec2-user/stock-history/gui_data/all.csv')
df['ESG_Score_en'] = df[['E_en', 'S_en', 'G_en']].mean(axis=1)
df['ESG_Score_loc'] = df[['E_loc', 'S_loc', 'G_loc']].mean(axis=1)
df['ESG_Score'] = df[['ESG_Score_en', 'ESG_Score_loc']].mean(axis=1)
df['Sentiment_en'] = df[['Sentiment_Token_en', 'Sentiment_Regex_en']].mean(axis=1)
df['Sentiment_loc'] = df[['Sentiment_Token_loc', 'Sentiment_Regex_loc']].mean(axis=1)

stock_data = investpy.get_stocks(country=country_code_for_investpy)
isins=df['ISIN'].drop_duplicates()
stock_data = stock_data[stock_data['isin'].apply(lambda x : x in isins.values)]
df['Full_Name'] = df['ISIN'].apply(lambda x: stock_data[stock_data['isin']==x].full_name.values[0])

df_gui = df[['Date','Full_Name','Sentiment_en','Sentiment_loc','ESG_Score']].rename(columns={'Full_Name':'Stock'})

df_gui.to_csv('/home/ec2-user/stock-history/gui_data/stock_gui_scores.csv', index = False, quotechar='"',quoting=csv.QUOTE_NONNUMERIC)

############### resample from monthly to daily ####################

df = pd.read_csv('/home/ec2-user/stock-history/gui_data/stock_gui_scores.csv')
df['Date'] = df['Date'].astype('datetime64[ns]')

def expand_dates(ser):
    return pd.DataFrame({'Date': pd.date_range(ser['Date'].min(), ser['Date'].max(), freq='D')})

newdf = df.groupby(['Stock']).apply(expand_dates).reset_index().merge(df, how='left')[['Date','Stock','Sentiment_en','Sentiment_loc','ESG_Score']].ffill()
#newdf = df.groupby(['Stock']).apply(expand_dates).reset_index().merge(df, how='left')[['Date', 'Stock', 'Sentiment']].ffill()

newdf.to_csv('/home/ec2-user/stock-history/gui_data/stock_gui_scores.csv')

