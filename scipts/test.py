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
