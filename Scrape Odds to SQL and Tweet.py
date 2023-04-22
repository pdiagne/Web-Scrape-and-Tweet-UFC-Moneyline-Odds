from icecream import ic
import requests
from datetime import datetime
import pytz
from bs4 import BeautifulSoup
import re
import pandas as pd
import dataframe_image as dfi
import difflib
import tweepy
import pyodbc

#*********************
# betus.com
#*********************

# Get data from url and record timestamp
url='https://www.betus.com.pa/sportsbook/martial-arts/mma/'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Select only UFC XYZ section of site
up_to = "ctl00_ctl00_M_middle_ConstructorLines1_GameLines1_repHeaders_ctl00_repLines_ctl00_pnlMarkets" # "UFC 283"
rx_to_first = r'^.*?{}'.format(re.escape(up_to))
soup_string = str(soup)
soup_string_t = re.sub(rx_to_first, '', soup_string, flags=re.DOTALL).strip()
head, sep, tail = soup_string_t.partition('ctl00_ctl00_M_middle_ConstructorLines1_GameLines1_repHeaders_ctl01_lblLeagueName')
soup = BeautifulSoup(head, 'html.parser')

# Fighter names
h_Names = soup.find_all('span', attrs={"id":"homeName"})
h_names_betus = []
for i in range(0,len(h_Names)):
    h_names_betus.append(h_Names[i].text.strip())

a_Names = soup.find_all('span', attrs={"id":"awayName"})
a_names_betus = []
for i in range(0,len(a_Names)):
    a_names_betus.append(a_Names[i].text.strip())
a_names_betus = list(map(lambda x: str.replace(x, "Anthony", "Antonio"), a_names_betus))

# Moneyline
h_Ml = soup.find_all('a', attrs={'id':re.compile("HomeMoneyLine")})
h_ml_betus = []
for i in range(0,len(h_Ml)):
    h_ml_betus.append(h_Ml[i].text.strip())

a_Ml = soup.find_all('a', attrs={'id':re.compile("VisitorMoneyLine")})
a_ml_betus = []
for i in range(0,len(a_Ml)):
    a_ml_betus.append(a_Ml[i].text.strip())

# Create output matrix
temp = pd.DataFrame({'h_names':h_names_betus,
                     'a_names':a_names_betus,
                     'h_ml':h_ml_betus,
                     'a_ml':a_ml_betus})
temp['UFC 283 Fighters'] = temp['h_names'] + "/" + temp['a_names']
temp['betus.com'] = temp['h_ml'] + "/" + temp['a_ml']
temp = temp[['UFC 283 Fighters','betus.com']]
temp_betus = temp

#*********************
# betnow.eu
#*********************

# Get data from url and record timestamp
url='https://www.betnow.eu/sportsbook-info/fighting/ufc/'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Select only UFC XYZ section of site
up_to = "UFC 283"
rx_to_first = r'^.*?{}'.format(re.escape(up_to))
soup_string = str(soup)
soup_string_t = re.sub(rx_to_first, '', soup_string, flags=re.DOTALL).strip()
head, sep, tail = soup_string_t.partition('UFC FIGHT NIGHT')
soup = BeautifulSoup(head, 'html.parser')

# Fighter names
Names = soup.find_all('span', attrs={"class":"team-name"})
names = []
for i in range(0,len(Names)):
    names.append(Names[i].text.strip())
names = [e[6:] for e in names]
h_names_betnow = names[::2]
a_names_betnow = names[1::2]
# Data cleaning
a_names_betnow = list(map(lambda x: str.replace(x, "Jr", "Jr."), a_names_betnow))
a_names_betnow = list(map(lambda x: str.replace(x, "Jair", "Jairzinho"), a_names_betnow))

# Moneyline
Ml = soup.find_all('span', attrs={"class":""})
ml = []
for i in range(0,len(Ml)):
    ml.append(Ml[i].text.strip())
ml = ml[2:]
ml = ml[::3]
h_ml_betnow = ml[::2]
a_ml_betnow = ml[1::2]

# Create output matrix
temp = pd.DataFrame({'h_names':h_names_betnow,
                     'a_names':a_names_betnow,
                     'h_ml':h_ml_betnow,
                     'a_ml':a_ml_betnow})
temp['UFC 283 Fighters'] = temp['a_names'] + "/" + temp['h_names']
temp['betnow.eu'] = temp['a_ml'] + "/" + temp['h_ml']
temp = temp[['UFC 283 Fighters','betnow.eu']]
temp_betnow = temp

# Combine results dataframes
for i in h_names_betnow:
    x = 0
    for j in h_names_betus:
        if i == j:
            temp_betnow.at[x, 'UFC 283 Fighters'] = a_names_betnow[x] + "/" + h_names_betnow[x]
            temp_betnow.at[x, 'betnow.eu'] = a_ml_betnow[x] + "/" + h_ml_betnow[x]
            x = x + 1
        else:
            x = x + 1
main = temp_betus.join(temp_betnow.set_index('UFC 283 Fighters'), on='UFC 283 Fighters')
main = main.dropna(subset = ['betnow.eu'])
main = main.dropna(subset = ['betus.com'])
#ic(main)

#*********************
# Export png, tweet, dataframe to database
#*********************

# Export results as png
tz_Pac = pytz.timezone('US/Pacific')
datetime_Pac = datetime.now(tz_Pac).strftime("%Y-%m-%d %H:%M")
blankIndex = ['']*len(main)
main.index=blankIndex
dfi.export(main, 'main.png')

# Twitter keys and tokens
api_key = ""
api_secrets = ""
access_token = ""
access_secret = ""

# Authenticate to Twitter
auth = tweepy.OAuthHandler(api_key, api_secrets)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

# Add text to image tweet
tweet_text= '#UFC283 #moneyline #betus #betnow'
image_path = "C:\\Users\\..." + "main.png"
status = api.update_status_with_media(tweet_text,"main.png")

# Setup dataframe for write to SQL Server
df = main
df.columns = ['fighters','betus','betnow']
df['event'] = "UFC 283"
df['date'] = datetime_Pac
df['id'] = df['date'] + df['fighters']
df = df.iloc[:,[5,4,0,1,2,3]]
df['date']= pd.to_datetime(df['date'], format = "%Y-%m-%d %H:%M")
df = df.dropna(subset = ['betnow'])
df = df.dropna(subset = ['betus'])

# Write dataframe to SQL Server
pyodbc.pooling = False
conn = pyodbc.connect(
   "DRIVER={SQL Server};SERVER=localhost\SQLEXPRESS;DATABASE=master;Trusted_connection=yes")
cursor = conn.cursor()
insert_records = '''INSERT INTO UFC(id, date, fighters, betus, betnow,event) VALUES(?,?,?,?,?,?) '''
for i in range(len(df)):
    cursor.execute(insert_records, df['id'].iat[i], df['date'].iat[i], df['fighters'].iat[i], df['betus'].iat[i],df['betnow'].iat[i], df['event'].iat[i])
conn.commit()
cursor.close()