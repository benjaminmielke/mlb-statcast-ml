import yaml
import pandas as pd
import numpy as np
import pickle


lst_brefid = 'troutmi01'
lst_year = '2020'
df = pd.read_html(f'https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid}&t=b&year={lst_year}')[4][['Date', 'BOP', 'Pos']]

df = df.drop(df.index[0:-2])
df = df.drop(df.index[-1])

df.tail(10)

filename = 'C:/Users/okiem/OneDrive/Desktop/Data Science Projects/Statcast/dicts/dct_bop_2021_test'
infile = open(filename, 'rb')
dct_bop_2021 = pickle.load(infile)
infile.close()
print(dct_bop_2021)

df.Date

df['Date']



pos_bop_stream.create_dct_bop()

from datetime import datetime, timedelta

print((datetime.now()-timedelta(1)).strftime('%Y-%m-%d'))


from sqlalchemy import create_engine

engine = create_engine(f'mysql+pymysql://root:Hh6994hh!@localhost/statcast')

engine.connect()
import pandas as pd
sql = 'SELECT * FROM statcast.wrk_statcast_2019;'

df_test = pd.read_sql_query(sql, engine)

lst1 = df_test['Result_Event'].unique()
lst2 = df_test['Event_Description'].unique()
lst1

lst_cnt_rbi = []
for i in range(0, len(df_test)):
    if df_test['Result_Event'][i] not in ['field_error', 'grounded_into_double_play', 'strikeout', 'strike_double_play', 'double_play']:
        lst_cnt_rbi.append(df_test['Post_Bat_Score'][i] - df_test['Bat_Score'][i])

lst_cnt_rbi

print(df_test['Gameday_Description'][11].count('scores'))

lst_post_bat_score = []
for i in range(0, len(df_test['Gameday_Description'])):
    if df_test['Gameday_Description'].iloc[i] is not None:
        runs_scored = float(df_test['Gameday_Description'].iloc[i].count('score')) + float(df_test['Gameday_Description'].iloc[i].count('homer'))
    else:
        runs_scored = 0
    lst_post_bat_score.append(float(df_test['Bat_Score'].iloc[i]) + runs_scored)

df_test.drop('Post_Bat_Score', inplace=True, axis=1)
pd.set_option('display.max_rows', 100)
df_score = df_test[['Bat_Score', 'Post_Bat_Score', 'Gameday_Description', ]]
df_score.head(100)

df_test.insert(df_test.columns.get_loc('Bat_Score'), 'Post_Bat_Score', lst_post_bat_score)
lst_post_bat_score = [bat_score + int(string.count('score')) for bat_score in df_test['Bat_Score'] for string in df_test['Gameday_Description'] if string is not None]
len(df_test['Bat_Score'])
len(lst_post_bat_score)
df_test['Gameday_Description'].sum.isna()
len(df_test)
lst_post_bat_score
df_test.head()
lst = list(df_test.index.values)
lst


import requests
from bs4 import BeautifulSoup
import pandas
import re
import json
url = 'https://baseballsavant.mlb.com/sprint_speed_leaderboard' #sprint speed leaderboard
site = requests.get(url) #gets the website from the internet
soup = BeautifulSoup(site, 'html.parser') #parses the page
dataStr = re.search('var data = (.+)[,;]{1}', str(soup.find_all('script')[10])) #grabs data from the javascript that loads in sprint speed data
# myJson = dataStr.group(1) #grabs data as Json file
# df = pandas.read_json(myJson) #reads into pandas dataframe

lst_df = pd.read_html('https://baseballsavant.mlb.com/leaderboard/sprint_speed?year=2020&position=&team=&min=10', flavor='bs4')


from selenium import webdriver
import pandas as pd

driver = webdriver.Chrome('C:/Users/okiem/Folders/Downloads/chromedriver_win32/chromedriver.exe')
driver.get("https://baseballsavant.mlb.com/leaderboard/sprint_speed?year=2017&position=&team=&min=0")

html = driver.page_source

tables = pd.read_html(html)
data = tables[0]

driver.close()


def move_word(s, word, pos):
    split = s.split()
    split.insert(pos, split.pop(split.index(word)))
    new_string = ' '.join(split).replace(',', '')
    if len(new_string.split()) == 3:
        new_string = new_string.replace(new_string.split()[2], '').rstrip()
        return new_string
    else:
        return new_string


lst = [move_word(x, x.split()[1], 0) for x in data['Player']]
data['Player'] = lst
data.head()




dct = dict(zip(data['Player'], data['Sprint Speed (ft / sec)']))
dct
playerstring = data['Player'].iloc[49]

newplayerstring = move_word(playerstring, playerstring.split()[1], 0)
newplayerstring
data.columns



df_test.columns
from sqlalchemy import create_engine
engine = create_engine(f'mysql+pymysql://root:Hh6994hh!@localhost/statcast')
engine.connect()
import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 100)
sql = 'SELECT * FROM statcast.wrk_statcast_2019;'
df_test = pd.read_sql_query(sql, engine)




df_test.groupby(['Batter_Name', 'Game_ID', 'Est_wOBA']).mean()


from bs4 import BeautifulSoup
import requests

url = requests.get('https://www.mlb.com/player/574847')

soup = bs.BeautifulSoup(url.content, 'html.parser')
soup.find_all('span', {'class' : 'player-header--vitals-name'})
name = BeautifulSoup(requests.get('https://www.mlb.com/player/5747').content, 'html.parser').find_all('span', {'class' : 'player-header--vitals-name'})[0].text.strip()
name

soup.find_all('span')
