from icecream import ic
import requests
from datetime import datetime
import pytz
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import dataframe_image as dfi
import difflib
import tweepy
import os

# Extract data from SQL Server
import pyodbc
pyodbc.pooling = False
conn = pyodbc.connect(
    "DRIVER={SQL Server};SERVER=localhost\SQLEXPRESS;DATABASE=master;Trusted_connection=yes")
cursor = conn.cursor()
retrieve_records = "select [date],[fighters],[betus],[betnow] from [master].[dbo].[UFC] where event like 'UFC 283' order by date"
d = pd.read_sql(retrieve_records, conn)
conn.commit()
cursor.close()

# Transform SQL output
d[['fighter_1','fighter_2']] = d['fighters'].str.split('/', expand = True)
d[['betus_1','betus_2']] = d['betus'].str.split('/', expand = True)
d[['betnow_1','betnow_2']] = d['betnow'].str.split('/', expand = True)
d = d.drop(['betnow', 'betus'], axis=1)

d = d[d['betus_1'].str.contains("Ev")==False]
d = d[d['betus_2'].str.contains("Ev")==False]
d = d[d['betnow_1'].str.contains("Ev")==False]
d = d[d['betnow_2'].str.contains("Ev")==False]

d = d.astype({'betus_1':'int'})
d = d.astype({'betus_2':'int'})
d = d.astype({'betnow_1':'int'})
d = d.astype({'betnow_2':'int'})

from plotly.subplots import make_subplots
import plotly.graph_objects as go

for i in range(len(d['fighters'].unique())):
    f = d['fighters'].iloc[i]
    temp = d[d['fighters'] == f]
    temp = temp.drop(['fighters','fighter_1','fighter_2'], axis=1)
    temp.columns = ['Date',d['fighter_1'].iloc[i]+'_betus',d['fighter_2'].iloc[i]+'_betus',d['fighter_1'].iloc[i]+'_betnow',d['fighter_2'].iloc[i]+'_betnow']
    cols = ['Date',d['fighter_1'].iloc[i]+'_betus',d['fighter_2'].iloc[i]+'_betus',d['fighter_1'].iloc[i]+'_betnow',d['fighter_2'].iloc[i]+'_betnow']
    temp = pd.DataFrame(data=temp)

    fig = make_subplots(rows=2, cols=1)
    fig.append_trace(go.Scatter(
        x=temp[cols[0]],
        y=temp[cols[1]],
        name=cols[1]),
        row=1, col=1)

    fig.append_trace(go.Scatter(
        x=temp[cols[0]],
        y=temp[cols[3]],
        name=cols[3]),
        row=1, col=1)

    fig.append_trace(go.Scatter(
        x=temp[cols[0]],
        y=temp[cols[2]],
        name=cols[2]),
        row=2, col=1)

    fig.append_trace(go.Scatter(
        x=temp[cols[0]],
        y=temp[cols[4]],
        name=cols[4]),
        row=2, col=1)

    fig.update_layout(title_text="UFC 283: "+f,legend=dict(orientation="h"))
    fig.write_image("UFC 283_"+str(i)+'.png')

import os
filelist = [file for file in os.listdir('images') if file.endswith('.png')]

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
filelist = [file for file in os.listdir("C:\\Users\\...") if file.endswith('.png')]
tweet_text= '#UFC282 #moneyline #betus #betnow'
image_path = "C:\\Users\\..." + filelist[i]
status = api.update_status_with_media(tweet_text,filelist[i])