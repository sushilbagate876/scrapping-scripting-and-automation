import pandas as pd
import numpy as np
import sys
import re
import investpy
import datetime
from datetime import timedelta

country_code_for_investpy = 'China'

df = pd.read_csv('/home/ec2-user/stock-history/gui_data/all.csv')
#df.fillna(0, inplace=True)
df['ESG_Score_en'] = df[['E_en', 'S_en', 'G_en']].mean(axis=1)
df['ESG_Score_loc'] = df[['E_loc', 'S_loc', 'G_loc']].mean(axis=1)
df['ESG_Score'] = df[['ESG_Score_en', 'ESG_Score_loc']].mean(axis=1)
df['Sentiment_en'] = df[['Sentiment_Token_en', 'Sentiment_Regex_en']].mean(axis=1)
df['Sentiment_loc'] = df[['Sentiment_Token_loc', 'Sentiment_Regex_loc']].mean(axis=1)

stock_data = investpy.get_stocks(country=country_code_for_investpy)
isins=df['ISIN'].drop_duplicates()
stock_data = stock_data[stock_data['isin'].apply(lambda x : x in isins.values)]

df_keywords = pd.read_csv('/home/ec2-user/stock-history/inputs/keywords_stocks.csv')
df['Stock'] = df['ISIN'].apply(lambda x: df_keywords[df_keywords['ISIN']==x].Full_Name.values[0])

############### resample from monthly to daily ####################

df['Date'] = df['Date'].astype('datetime64[ns]')
def expand_dates(ser):
    return pd.DataFrame({'Date': pd.date_range(ser['Date'].min(), ser['Date'].max(), freq='D')})

newdf = df.groupby(['Stock']).apply(expand_dates).reset_index().merge(df, how='left')[['Date','ISIN','Stock','Sentiment_en','Sentiment_loc','ESG_Score']].ffill()

################## sector and returns ##################

sector_data = pd.read_csv('/home/ec2-user/stock-history/master_profile_2.csv')
sector_data = sector_data[sector_data['isin'].apply(lambda x : x in isins.values)]

####### Get Prices data till today
today = datetime.datetime.now().strftime("%d/%m/%Y")

df_prices = pd.DataFrame()
for i in sector_data.index:
    try:
        prices = investpy.get_stock_historical_data(stock=sector_data['symbol'][i],country=sector_data['country'][i],from_date='01/01/2016',to_date=today)
        prices['Stock'] = sector_data['full_name'][i]
        prices['ISIN'] = sector_data['isin'][i]
        prices['Returns'] = np.log(prices['Close'] / prices['Close'].shift())
        prices.reset_index(inplace=True)
        df_prices = df_prices.append(prices)
    except:
        pass

####### add Country, Sector, Industry Columns to stock_gui_scores.csv
newdf['Country'] = country_code_for_investpy
newdf['Sector'] = newdf['ISIN'].apply(lambda x: sector_data[sector_data['isin']==x].sector.values[0])
newdf['Industry'] = newdf['ISIN'].apply(lambda x: sector_data[sector_data['isin']==x].industry.values[0])
newdf['Date'] = pd.to_datetime(newdf['Date'])

DF = pd.merge(newdf, df_prices, how='left')
columns = ['Date','Stock','Sentiment_en','Sentiment_loc','ESG_Score','Country','Sector','Industry','Returns']

DF.to_csv('/home/ec2-user/stock-history/gui_data/stock_gui_scores.csv',index = False,columns = columns)

