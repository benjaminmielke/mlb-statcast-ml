#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import urllib
from sqlalchemy import create_engine
from pybaseball import statcast
from datetime import datetime
import tkinter as tk
import logging
import ujson
import yaml
from bs4 import BeautifulSoup
import requests


class Statcast_DB():
    '''
    Used to build a data pipeline for MLB Statcast data. With
    this class, there is access to a method(build_db()) to build
    a database with raw and tranformed statcast data. Once the
    database is built, a method(stream_data()) can be used to append
    new data through the pipeline during in-season.

    Args:
        None

    Attributes:
        playerMap(DataFrame): A map containing info for all players to be
                              used for data transformation.
        dct_playerIDs(Dictionary): Dictionary with the player MLB IS as the
                                   key, and the player MLB Full Name as the
                                   value.
        team_atts(DataFrame): A csv file that contains descriptive information
                              for each team; Ball park for each year, league,
                              and division.
        dct_team_league(Dictionary): A dictionary with the team as the key and
                                     the league as the value.
        dct_team_division(Dictionary): A dictionary with the team as the key and
                                      the division as the value.

    '''
    def __init__(self):

        with open('config.yaml', 'r') as yamlfile:
            self.lst_config = yaml.load(yamlfile, Loader=yaml.FullLoader)

        self.parent_path = self.lst_config[3]['Paths']['parent_path']
        self.playerMap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV',
                                     usecols=['MLBID', 'MLBNAME', 'BREFID', 'POS', 'PLAYERNAME'],
                                     dtype={'MLBID': 'category', 'MLBNAME': 'category'},
                                     skiprows=[2604+1])
        self.dct_playerIDs = dict(zip(self.playerMap['MLBID'], self.playerMap['MLBNAME']))
        self.team_atts = pd.read_csv(f'{self.parent_path}/csv/team_atts.csv')
        self.dct_team_league = dict(zip(self.team_atts['Team'], self.team_atts['League']))
        self.dct_team_division = dict(zip(self.team_atts['Team'], self.team_atts['Division']))


        print('To Build an initial Database, call build_db()')
        print('To append date(s) into Database, call stream_data("yyyy-mm-dd")')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_variables_mssql(self):
        '''A method for the button in the prompt window to call that retreives the entry
        values for the database information.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        self.dbtype = 'mssql'
        self.driver = self.entry_b.get()
        self.server = self.entry_c.get()
        self.database = self.entry_d.get()
        self.connect_db()
        self.window.destroy()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_variables_mysql(self):
        '''A method for the button in the prompt window to call that retreives the entry
        values for the database information.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        self.dbtype = 'mysql'
        self.username = self.entry_a.get()
        self.password = self.entry_b.get()
        self.server = self.entry_c.get()
        self.database = self.entry_d.get()
        self.connect_db()
        self.window.destroy()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def mssql_window(self):
        '''Opens a prompt window for the user to enter the MSSQL Server database information that
        will be used for the data to be entered into.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        self.window.destroy()
        self.window = tk.Tk()
        self.window.geometry('500x180')
        self.window.config(bg='#0E4B95')
        self.window.title('MLB StatCast DB Builder')

        frame_m = tk.Frame(master=self.window, width=400, height=25)
        frame_m.pack()
        frame_b = tk.Frame(master=self.window, width=400, height=25)
        frame_b.pack(pady=2)
        frame_c = tk.Frame(master=self.window, width=400, height=25)
        frame_c.pack(pady=2)
        frame_d = tk.Frame(master=self.window, width=400, height=25)
        frame_d.pack(pady=2)
        frame_e = tk.Frame(master=self.window, width=400, height=25)
        frame_e.pack(pady=2)

        label_m = tk.Label(master=frame_m,
                           text='Enter MSSQL Server Database Information',
                           fg='white',
                           pady=5,
                           bg='#0E4B95')
        label_m.pack()

        label_b = tk.Label(master=frame_b,
                           text='Enter Driver:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_b.pack(side='left')
        self.entry_b = tk.Entry(master=frame_b,
                                width='50',
                                fg='#9F0808')
        self.entry_b.pack(side='right')
        self.entry_b.insert(0, '{SQL SERVER}')

        label_c = tk.Label(master=frame_c,
                           text='Enter Sever Name:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_c.pack(side='left')
        self.entry_c = tk.Entry(master=frame_c,
                                width='50',
                                fg='#9F0808')
        self.entry_c.pack(side='right')
        self.entry_c.insert(0, 'DESKTOP-BMEMTUQ\SQLEXPRESS')

        label_d = tk.Label(master=frame_d,
                           text='Enter Database:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_d.pack(side='left')
        self.entry_d = tk.Entry(master=frame_d,
                                width='50',
                                fg='#9F0808')
        self.entry_d.pack(side='right')
        self.entry_d.insert(0, 'STATCAST')

        button_get = tk.Button(master=frame_e,
                               text='CONNECT and BUILD',
                               command=self.get_variables_mssql,
                               bg='#9F0808',
                               fg='white')
        button_get.pack(pady=3)

        self.window.eval('tk::PlaceWindow . center')
        self.window.mainloop()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def mysql_window(self):
        '''Opens a prompt window for the user to enter the MySQL database information that
        will be used for the data to be entered into.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        self.window.destroy()
        self.window = tk.Tk()
        self.window.geometry('500x180')
        self.window.config(bg='#0E4B95')
        self.window.title('MLB StatCast DB Builder')

        frame_m = tk.Frame(master=self.window, width=400, height=25)
        frame_m.pack()
        frame_a = tk.Frame(master=self.window, width=400, height=25)
        frame_a.pack(pady=2)
        frame_b = tk.Frame(master=self.window, width=400, height=25)
        frame_b.pack(pady=2)
        frame_c = tk.Frame(master=self.window, width=400, height=25)
        frame_c.pack(pady=2)
        frame_d = tk.Frame(master=self.window, width=400, height=25)
        frame_d.pack(pady=2)
        frame_e = tk.Frame(master=self.window, width=400, height=25)
        frame_e.pack(pady=2)

        label_m = tk.Label(master=frame_m,
                           text='Enter MySQL Database Information',
                           fg='white',
                           pady=5,
                           bg='#0E4B95')
        label_m.pack()

        label_a = tk.Label(master=frame_a,
                           text='Enter Username:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_a.pack(side='left')
        self.entry_a = tk.Entry(master=frame_a,
                                width='50',
                                fg='#9F0808')
        self.entry_a.pack(side='right')
        self.entry_a.insert(0, 'root')

        label_b = tk.Label(master=frame_b,
                           text='Enter Password:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_b.pack(side='left')
        self.entry_b = tk.Entry(master=frame_b,
                                width='50',
                                fg='#9F0808')
        self.entry_b.pack(side='right')
        self.entry_b.insert(0, 'Hh6994hh!')

        label_c = tk.Label(master=frame_c,
                           text='Enter Sever:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_c.pack(side='left')
        self.entry_c = tk.Entry(master=frame_c,
                                width='50',
                                fg='#9F0808')
        self.entry_c.pack(side='right')
        self.entry_c.insert(0, 'localhost')

        label_d = tk.Label(master=frame_d,
                           text='Enter Database:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_d.pack(side='left')
        self.entry_d = tk.Entry(master=frame_d,
                                width='50',
                                fg='#9F0808')
        self.entry_d.pack(side='right')
        self.entry_d.insert(0, 'statcast')

        button_get = tk.Button(master=frame_e,
                               text='CONNECT and BUILD',
                               command=self.get_variables_mysql,
                               bg='#9F0808',
                               fg='white')
        button_get.pack(pady=3)

        self.window.eval('tk::PlaceWindow . center')
        self.window.mainloop()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def prompt_window(self):
        '''

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        self.window = tk.Tk()
        self.window.geometry('500x150')
        self.window.config(bg='#0E4B95')
        self.window.title('MLB StatCast DB Builder')

        frame_a = tk.Frame(master=self.window, width=400, height=25)
        frame_a.pack()
        frame_b = tk.Frame(master=self.window, width=400, height=25)
        frame_b.pack(pady=2)

        label_a = tk.Label(master=frame_a,
                           text='Welcome to the MLB StatCast Database Builder!\nWhat type of database are you building?',
                           pady=3)
        label_a.pack()

        button_mssql = tk.Button(master=frame_b,
                                 text='MSSQL Server',
                                 command=self.mssql_window,
                                 bg='#9F0808',
                                 fg='white')
        button_mssql.pack(padx=2, side='left')
        button_mysql = tk.Button(master=frame_b,
                                 text='MySQL',
                                 command=self.mysql_window,
                                 bg='#9F0808',
                                 fg='white')
        button_mysql.pack(padx=2, side='right')

        self.window.eval('tk::PlaceWindow . center')
        self.window.mainloop()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def connect_db(self):
        '''Connects and creates an engine for the SQL Database specified
        by the user.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        if self.dbtype == 'mssql':
            self.driver = self.lst_config[1]['DB_MSSQL']['driver']
            self.server = self.lst_config[1]['DB_MSSQL']['server']
            self.database = self.lst_config[1]['DB_MSSQL']['database']
            params = urllib.parse.quote_plus(f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database}")

            self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect=%s" % params)

        if self.dbtype == 'mysql':
            self.username = self.lst_config[2]['DB_MySQL']['username']
            self.password = self.lst_config[2]['DB_MySQL']['password']
            self.server = self.lst_config[2]['DB_MySQL']['server']
            self.database = self.lst_config[2]['DB_MySQL']['database']

            self.engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.server}/{self.database}').connect()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def reorder_columns(self):
        '''Reorders the columns in the dataframe into a logical order.

        The method uses a text file that contains the ordered column list.
        The columns are ordered in such a way that it is logical and
        chronological. With an oder showing the pre-pitch environment,
        then during-pitch information, then post-pitch event results,
        then statistics derived from the event.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        with open(f'{self.parent_path}/lists/lst_reorder_cols.txt', 'r') as filehandler:
            lst_reorder_columns = [line[:-1] for line in filehandler]

        self.df = self.df[lst_reorder_columns]

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def rename_columns(self):
        '''Renames the columns to be cleaner and add descriptive value.

        The columns names that are used in the raw statcast data are not
        intuitive in the least bit. This method remedies that problem.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        with open(f'{self.parent_path}/lists/lst_rename_cols.txt', 'r') as filehandler:
            lst_rename_columns = [line[:-1] for line in filehandler]

        self.df.columns = lst_rename_columns

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_batter_name(self):
        '''Adds a column that contains the batters full name.

        The raw statcast data only shows the batter's MLB ID number.
        This method uses a MLB Player Map to create a new column with
        the associated batter name to the ID. It uses a dictionary that
        is created in the __init__ method.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''

        lst_batter_name = [self.dct_playerIDs.get(x, None) for x in self.df['Batter_ID']]
        self.df.insert(self.df.columns.get_loc('Pitcher_ID'),
                       'Batter_Name',
                       lst_batter_name)

        # lst_batter_name = []
        # for p in range(0, len(self.df['Batter_ID'])):
        #     if self.df['Batter_ID'].iloc[p] in self.dct_playerIDs:
        #         batter_name = self.dct_playerIDs.get(self.df['Batter_ID'].iloc[p], None)
        #     else:
        #         try:
        #             batter_name = BeautifulSoup(requests.get(f'https://www.mlb.com/player/{self.df["Batter_ID"].iloc[p]}').content, 'html.parser').find_all('span', {'class': 'player-header--vitals-name'})[0].text.strip()
        #         except IndexError:
        #             logging.info(f'Player ID: {self.df["Batter_ID"][p]}: not found in playeridmap or mlb.com!')
        #             continue
        #
        #     lst_batter_name.append(batter_name)
        #
        # self.df.insert(self.df.columns.get_loc('Pitcher_ID'),
        #                'Batter_Name',
        #                lst_batter_name)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_player_names(self):
        '''Replaces the MLB IDs with Player Full Names in all the Fielder_* columns and
        On_* columns.

        The raw statcast data shows only the MLD ID for all players that are in the field
        and on base during each pitch. This method replaces the MLB IDs with the PLayer
        Full Names using a dictionary created in __init__.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        lst_cols = ['Fielder_2', 'Fielder_3', 'Fielder_4', 'Fielder_5', 'Fielder_6',
                    'Fielder_7', 'Fielder_8', 'Fielder_9', 'On_1b', 'On_2b', 'On_3b']

        for c in range(0, len(lst_cols)):
            new_col = [self.dct_playerIDs.get(x, None) for x in self.df[lst_cols[c]]]
            self.df[lst_cols[c]] = new_col

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_teams(self):
        '''Adds columns that show the Batting and Fielding Teams.

        The raw statcast data only shows the home and away teams, and
        does not explicitly specify which team the pitcher and hitter
        are on. This method deduces which team the pitcher and hitter
        are on and creates a column.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        lst_bat_team = []
        lst_fld_team = []

        for i in range(0, len(self.df['Inning_TopBot'])):
            if self.df['Inning_TopBot'][i] == 'Top':
                lst_bat_team.append(self.df['Away_Team'][i])
                lst_fld_team.append(self.df['Home_Team'][i])
            if self.df['Inning_TopBot'][i] == 'Bot':
                lst_bat_team.append(self.df['Home_Team'][i])
                lst_fld_team.append(self.df['Away_Team'][i])

        self.df.insert(self.df.columns.get_loc('Home_Score'),
                       'Bat_Team',
                       lst_bat_team)
        self.df.insert(self.df.columns.get_loc('Home_Score'),
                       'Fld_Team',
                       lst_fld_team)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_lg_div(self):
        '''Adds columns that show the league and division for the
        pitcher and batter teams.

        This method uses a pre-made csv file that contains an array
        of descrptive information for each team.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        lst_bat_league = [self.dct_team_league.get(x, None) for x in self.df['Bat_Team']]
        lst_bat_division = [self.dct_team_division.get(x, None) for x in self.df['Bat_Team']]
        lst_fld_league = [self.dct_team_league.get(x, None) for x in self.df['Fld_Team']]
        lst_fld_division = [self.dct_team_division.get(x, None) for x in self.df['Fld_Team']]

        self.df.insert(self.df.columns.get_loc('Home_Score'),
                       'Bat_Team_League',
                       lst_bat_league)
        self.df.insert(self.df.columns.get_loc('Home_Score'),
                       'Bat_Team_Division',
                       lst_bat_division)
        self.df.insert(self.df.columns.get_loc('Home_Score'),
                       'Fld_Team_League',
                       lst_fld_league)
        self.df.insert(self.df.columns.get_loc('Home_Score'),
                       'Fld_Team_Division',
                       lst_fld_division)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_ballparks(self, year):
        '''Adds a column that shows the ball park.

        This method uses a pre-made csv file that contains an array
        of descrptive information for each team.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        lst_parks = [self.dct_parks.get(x, None) for x in self.df['Home_Team']]
        self.df.insert(self.df.columns.get_loc('Home_Score'),
                       'Ball_Park',
                       lst_parks)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_batter_pos(self):
        '''Adds a column that shows what position the batter was playing
        during the game the pitch was thrown.

        This method uses a pre-made, extensive dictionary that contains what
        position a player was playing for each game in their career. The dictionary was created by scraping baseball-reference.com.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        lst_hitter_pos = []
        for i in range(0, len(self.df['Batter_Name'])):
            try:
                lst_hitter_pos.append(self.dct_pos[self.df['Batter_Name'][i]].get(str(self.df['Game_Date'][i])[:10], None))
            except:
                lst_hitter_pos.append('P')
                continue
        self.df.insert(self.df.columns.get_loc('Home_Team'),
                       'Batter_Pos',
                       lst_hitter_pos)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_batter_bop(self):
        '''Adds a column that shows what position in the batter order
        that batter was in for each pitch.

        This method uses a pre-made, extensive dictionary that contains what
        position in the batting order a player was in for each game in their career.
        The dictionary was created by scraping baseball-reference.com.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        lst_hitter_bop = []
        for i in range(0, len(self.df['Batter_Name'])):
            try:
                lst_hitter_bop.append(self.dct_bop[self.df['Batter_Name'][i]].get(str(self.df['Game_Date'][i])[:10], None))
            except:
                lst_hitter_bop.append(9)
                continue
        self.df.insert(self.df.columns.get_loc('Home_Team'),
                       'Batter_BOP',
                       lst_hitter_bop)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_batter_ss(self):
        '''
        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''

        lst_hitter_ss = [self.dct_ss.get(batter, None) for batter in self.df['Batter_Name']]
        self.df.insert(self.df.columns.get_loc('Home_Team'),
                       'Batter_Sprint',
                       lst_hitter_ss)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_post_scores(self):
        lst_post_bat_score = []
        for i in range(0, len(self.df['Gameday_Description'])):
            if self.df['Gameday_Description'].iloc[i] is not None:
                runs_scored = float(self.df['Gameday_Description'].iloc[i].count('score')) + float(self.df['Gameday_Description'].iloc[i].count('homer'))
            else:
                runs_scored = 0
            lst_post_bat_score.append(float(self.df['Bat_Score'].iloc[i]) + runs_scored)

        self.df.insert(self.df.columns.get_loc('Inning'),
                       'Post_Bat_Score',
                       lst_post_bat_score)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def stream_pos_bop_dct(self, date):
        '''
        Create dictionary json files for each year containing which field position
         a player was playing for each date in their career.

        This function uses a MLB PlayerID Map and baseball-reference.com game-logs
        to create yearly dictionaries containing which field position a player was
        playing for each game in their career. The function scrapes data from
        the baseball-reference.com game logs for each player then parses and
        converts the data into the form neccessary to use in a Statcast Databse.

        Example of an entry into a dictionary:
        {"Jose Abreu": {"2020-07-24": "3", "2020-07-25": "3" ...

        Args:
            No arguments.

        Returns:
            list: a list of each years dictionary

        '''
        with open('config.yaml', 'r') as yamlfile:
            lst_config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        parent_path = lst_config[3]['Paths']['parent_path']

        # date = (datetime.now()-timedelta(1)).strftime('%Y-%m-%d')
        # date = '2019-06-03'
        year = date[:4]

        self.dct_pos = {}
        self.dct_bop = {}

        filename_pos = f'{parent_path}/dicts/dct_pos_2021_test.txt'
        filename_bop = f'{parent_path}/dicts/dct_bop_2021_test.txt'

        playeridmap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV')

        try:
            self.dct_pos = ujson.load(open(filename_pos))
        except ValueError:
            print('The Pos text file is empty.')
        try:
            self.dct_bop = ujson.load(open(filename_bop))
        except ValueError:
            print('The BOP text file is empty.')

        playeridmap_hitters = playeridmap.drop(playeridmap[playeridmap['POS'].str.contains('P')].index)
        playeridmap_hitters = playeridmap.drop(playeridmap[playeridmap['ACTIVE'].str.contains('N')].index)
        playeridmap_hitters = playeridmap_hitters.reset_index(drop=True)
        dct_brefid_name = dict(zip(playeridmap_hitters['BREFID'], playeridmap_hitters['PLAYERNAME']))
        lst_brefid = playeridmap_hitters['BREFID']
        for p in range(0, len(lst_brefid)):
            try:
                df = pd.read_html(f'https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={year}')[4][['Date', 'BOP', 'Pos']]
            except:
                print(f'Table Not Found: {dct_brefid_name.get(lst_brefid[p])}:{year}: https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={year}')
                continue

            df.drop(df.index[0:-2], inplace=True)
            df.drop(df.index[-1], inplace=True)

            df['Date'] = df.Date.map(str) + " " + year
            df.drop(df[df['Date'].str.contains('Date')].index, inplace=True)
            df.drop(df[df['Date'].str.contains('nan')].index, inplace=True)
            df = df.reset_index(drop=True)

            df['Date'] = df['Date'].str.replace('(1)', '', regex=False)
            df['Date'] = df['Date'].str.replace('(2)', '', regex=False)
            df['Date'] = df['Date'].str.replace('susp', '', regex=False)
            df.loc[0, 'Date'] = datetime.strptime(df.loc[0, 'Date'], '%b %d %Y').strftime('%Y-%m-%d')

            print(df.loc[0, 'Date'])
            print(date)

            if df.loc[0, 'Date'] == date:
                if dct_brefid_name.get(lst_brefid[p]) in self.dct_pos:
                    self.dct_pos.get(dct_brefid_name.get(lst_brefid[p])).update(dict(zip(df['Date'], df['Pos'])))
                    print(f'Updated Pos: {dct_brefid_name.get(lst_brefid[p])}: entry updated for {date}')
                else:
                    self.dct_pos.update({dct_brefid_name.get(lst_brefid[p]): dict(zip(df['Date'], df['Pos']))})
                    print(f'Inserted Pos: {dct_brefid_name.get(lst_brefid[p])}: new entry inserted for {date}')

                if dct_brefid_name.get(lst_brefid[p]) in self.dct_bop:
                    self.dct_bop.get(dct_brefid_name.get(lst_brefid[p])).update(dict(zip(df['Date'], df['BOP'])))
                    print(f'Updated BOP: {dct_brefid_name.get(lst_brefid[p])}: entry updated for {date}')
                else:
                    self.dct_bop.update({dct_brefid_name.get(lst_brefid[p]): dict(zip(df['Date'], df['BOP']))})
                    print(f'Inserted BOP: {dct_brefid_name.get(lst_brefid[p])}: new entry inserted for {date}')
            else:
                print(f'DNP: {dct_brefid_name.get(lst_brefid[p])}: Did not play {date}')

        ujson.dump(self.dct_pos, open(filename_pos, 'w'))
        ujson.dump(self.dct_bop, open(filename_bop, 'w'))

        print(f'Pos Dictionary Completed for {date}')
        print(f'BOP Dictionary Completed for {date}')

        return self.dct_pos, self.dct_bop

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_pa(self):
        self.df['cnt_PA'] = np.select([
            self.df['Result_Event'] == 'strikeout',
            self.df['Result_Event'] == 'field_out',
            self.df['Result_Event'] == 'grounded_into_double_play',
            self.df['Result_Event'] == 'strikeout_double_play',
            self.df['Result_Event'] == 'double_play',
            self.df['Result_Event'] == 'force_out',
            self.df['Result_Event'] == 'fielders_choice_out',
            self.df['Result_Event'] == 'fielders_choice',
            self.df['Result_Event'] == 'single',
            self.df['Result_Event'] == 'double',
            self.df['Result_Event'] == 'triple',
            self.df['Result_Event'] == 'home_run',
            self.df['Result_Event'] == 'hit_by_pitch',
            self.df['Result_Event'] == 'walk',
            self.df['Result_Event'] == 'field_error',
            self.df['Result_Event'] == 'sac_fly',
            self.df['Result_Event'] == 'sac_bunt',
            self.df['Result_Event'] == 'interf_def'],
            [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_ab(self):
        self.df['cnt_AB'] = np.select([
            self.df['Result_Event'] == 'strikeout',
            self.df['Result_Event'] == 'field_out',
            self.df['Result_Event'] == 'grounded_into_double_play',
            self.df['Result_Event'] == 'strikeout_double_play',
            self.df['Result_Event'] == 'double_play',
            self.df['Result_Event'] == 'force_out',
            self.df['Result_Event'] == 'fielders_choice_out',
            self.df['Result_Event'] == 'fielders_choice',
            self.df['Result_Event'] == 'single',
            self.df['Result_Event'] == 'double',
            self.df['Result_Event'] == 'triple',
            self.df['Result_Event'] == 'home_run'],
            [1,1,1,1,1,1,1,1,1,1,1,1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_hit(self):
        self.df['cnt_Hit'] = np.select([
            self.df['Result_Event'] == 'single',
            self.df['Result_Event'] == 'double',
            self.df['Result_Event'] == 'triple',
            self.df['Result_Event'] == 'home_run'],
            [1,1,1,1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_single(self):
        self.df['cnt_Single'] = np.select([
            self.df['Result_Event'] == 'single'],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_double(self):
        self.df['cnt_Double'] = np.select([
            self.df['Result_Event'] == 'double'],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_triple(self):
        self.df['cnt_Triple'] = np.select([
            self.df['Result_Event'] == 'triple'],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_home_run(self):
        self.df['cnt_Home_Run'] = np.select([
            self.df['Result_Event'] == 'home_run'],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_k(self):
        self.df['cnt_K'] = np.select([
            (self.df['Result_Event'] == 'strikeout') & (self.df['Event_Description'] == 'called_strike')],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_backwardsk(self):
        self.df['cnt_BackwardsK'] = np.select([
            (self.df['Result_Event'] == 'strikeout') & (self.df['Event_Description'] == 'swinging_strike')],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_walk(self):
        self.df['cnt_Walk'] = np.select([
            self.df['Result_Event'] == 'walk'],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_hbp(self):
        self.df['cnt_HBP'] = np.select([
            self.df['Result_Event'] == 'hit_by_pitch'],
            [1], default=0)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_cnt_rbi(self):
        lst_cnt_rbi = []
        for i in range(0, len(self.df)):
            if self.df['Result_Event'][i] not in ['field_error', 'grounded_into_double_play', 'strikeout', 'strikeout_double_play', 'double_play']:
                lst_cnt_rbi.append(self.df['Post_Bat_Score'][i] - self.df['Bat_Score'][i])
            else:
                lst_cnt_rbi.append(0)

        self.df.insert(len(self.df.columns),
                       'cnt_RBI',
                       lst_cnt_rbi)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def calc_yahoo_pnts(self, i):
        pnts_b = (self.df['cnt_Single'].iloc[i]*2.6) + (self.df['cnt_Double'].iloc[i]*5.2) + (self.df['cnt_Triple'].iloc[i]*7.8) + (self.df['cnt_Home_Run'].iloc[i]*10.4) + (self.df['cnt_RBI'].iloc[i]*1.9) + (self.df['cnt_Walk'].iloc[i]*2.6) + (self.df['cnt_HBP'].iloc[i]*2.6)

        return pnts_b

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_yahoo_pnts(self):
        lst_yahoo_pnts_b = [self.calc_yahoo_pnts(i) for i in list(self.df.index.values)]
        self.df.insert(len(self.df.columns), 'Yahoo_Pnts_Batter', lst_yahoo_pnts_b)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_pitcher_type(self):
        self.df.insert(self.df.columns.get_loc('Pitcher_Hand'), 'Pitcher_Type', None)
        self.df.set_index('Game_ID',
                          inplace=True,
                          drop=True)
        self.df.sort_values(axis=0,
                            by=['Game_ID', 'PA_Num', 'Pitch_Num'],
                            ascending=[False, True, True],
                            inplace=True)
        lst_gameids = self.df.index.unique()
        for i in range(0, len(lst_gameids)):
            lst_game_pitcher = list(self.df.loc[lst_gameids[i], 'Pitcher_Name'].unique())
            lst_pitcher_pos = list(self.df.loc[lst_gameids[i]]['Pitcher_Type'].values)

            for p in range(0, len(self.df.loc[lst_gameids[i], 'Pitcher_Name'])):
                if self.df.loc[lst_gameids[i], 'Pitcher_Name'].values[p] in [lst_game_pitcher[0], lst_game_pitcher[1]]:
                    lst_pitcher_pos[p] = 'SP'
                else:
                    lst_pitcher_pos[p] = 'RP'

            self.df.loc[lst_gameids[i], 'Pitcher_Type'] = lst_pitcher_pos


# ------------------------------------------------------------------------------
# ++++++++++++++Builder Method+++++++++++++++++++++++++++++++++++++++++++++++++
# -----------------------------------------------------------------------------

    def build_db(self):
        '''Builds MLB Statcast tables within a pre-defined SQL database.

        Through a pipeline, this method will build tables containing all available
        MLB statcast data. It will will use the SQL sever and database
        information provided by the user within the GUI prompt that pops-up upon
        calling the method. Each year with have a RAW table created and a WRK
        table created. The RAW data is the untouched data from Baseball Savant,
        while the WRK table is the organized data with increased level of detail.
        This method will likely take several hours to complete. There is a log file
        that is created to track the progress. The data tables are created day-by-day,
        with each day appended. This should limit memory usage.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            Exception: if the server information is not correct/exists

        '''
        startTime = datetime.now()


        logging.basicConfig(filename=f'{self.parent_path}/logs/build_db.log',
                            filemode='w',
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%d-%b-%y %H:%M:%S',
                            level=logging.INFO)

        try:
            self.prompt_window()
            logging.info(f'Connected to: \nServer: {self.server}\nDatabase: {self.database}')
        except Exception as e:
            logging.exception('Exception occured')

        lst_year = ['2016', '2017', '2018', '2019']
        lst_month = ['03', '09']
        lst_day_31 = ['01', '31']
        lst_day_30 = ['01','30']

#         lst_year =  list(reversed([str(i) for i in range(2008,2021)]))
#         lst_month = ['0'+str(i) for i in range(3,10)] + [str(i) for i in range(10,12)]
#         lst_day_31 = ['0'+str(i) for i in range(1,10)] + [str(i) for i in range(10,32)]
#         lst_day_30 = ['0'+str(i) for i in range(1,10)] + [str(i) for i in range(10,31)]



        for y in range(0, len(lst_year)):
            self.dct_pos = ujson.load(open(f'{self.parent_path}/dicts/dct_pos_{lst_year[y]}.txt'))
            self.dct_bop = ujson.load(open(f'{self.parent_path}/dicts/dct_bop_{lst_year[y]}.txt'))
            self.dct_ss = ujson.load(open(f'{self.parent_path}/dicts/dct_ss_{lst_year[y]}.txt'))
            self.dct_parks = dict(zip(self.team_atts['Team'], self.team_atts[f'Ball_Park_{lst_year[y]}']))

            for m in range(0, len(lst_month)):
                if any(lst_month[m] == x for x in ['03', '05', '07', '08', '10']):
                    lst_day = lst_day_31
                else:
                    lst_day = lst_day_30
                for d in range(0, len(lst_day)):

                    try:
                        self.df = pd.DataFrame(statcast(start_dt=f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}',
                                                        end_dt=f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}'))
                    except Exception as e:
                        logging.exception("Exception occurred")

                    self.df = self.df.replace({np.nan: None})
                    self.df = self.df.astype({'batter': 'int32'})

                    if self.df.empty:
                        print(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: NO DATA FOR THIS DATE')
                        logging.warning(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: NO DATA FOR THIS DATE')
                    else:
                        print('............')
                        print(f'|{lst_year[y]}/{lst_month[m]}/{lst_day[d]}|')
                        print('............')
                        print(f'Completed: RAW data imported from Baseball Savant')
                        logging.info(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: RAW data imported from Baseball Savant')

                        try:
                            self.df.drop(['index', 'pitcher.1', 'fielder_2.1', 'post_away_score', 'post_home_score', 'post_bat_score', 'post_fld_score'],
                                         axis=1,
                                         inplace=True)
                            self.df.to_sql(f'raw_statcast_{lst_year[y]}',
                                           self.engine,
                                           index=False,
                                           if_exists='append')
                            print(f'Completed: Raw data inserted into DB: STATCAST , TABLE: raw_statcast_{lst_year[y]}')
                            logging.info(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Raw data inserted into DB: STATCAST , TABLE: raw_statcast_{lst_year[y]}')
                        except Exception as e:
                            logging.exception("Exception occurred")

                        try:
                            self.reorder_columns()
                            self.rename_columns()
                            self.add_batter_name()
                            self.add_player_names()
                            self.add_teams()
                            self.add_lg_div()
                            self.add_ballparks(lst_year[y])
                            self.add_batter_pos()
                            self.add_batter_bop()
                            self.add_batter_ss()
                            self.add_post_scores()
                            self.add_cnt_pa()
                            self.add_cnt_ab()
                            self.add_cnt_hit()
                            self.add_cnt_single()
                            self.add_cnt_double()
                            self.add_cnt_triple()
                            self.add_cnt_home_run()
                            self.add_cnt_k()
                            self.add_cnt_backwardsk()
                            self.add_cnt_walk()
                            self.add_cnt_hbp()
                            self.add_cnt_rbi()
                            self.add_yahoo_pnts()
                            self.add_pitcher_type()

                            print(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Data transformation complete')
                            logging.info(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Data transformation complete')
                        except Exception as e:
                            logging.exception('Exception Occured')

                        try:
                            # self.df.reset_index(inplace=True)
                            self.df.to_sql(f'wrk_statcast_{lst_year[y]}',
                                           self.engine,
                                           index=True,
                                           if_exists='append')
                            print(f'Completed: Working data inserted into DB: STATCAST , TABLE: wrk_statcast_{lst_year[y]}')
                            print(f'Time Elapsed: {datetime.now() - startTime}\n')
                            logging.info(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Working data inserted into DB: STATCAST , TABLE: wrk_statcast_{lst_year[y]}\nTime Elapsed: {datetime.now() - startTime}')
                        except Exception as e:
                            logging.exception("Exception occurred")

        print('\n\n--------All Dates Complete--------')
        print(f'Total Time Elapsed: {datetime.now() - startTime}')
        logging.info(f'--------All Dates Complete--------\nTotal Time Elapsed: {datetime.now() - startTime}')

# ------------------------------------------------------------------------------
# ++++++++++++Streaming Method+++++++++++++++++++++++++++++++++++++++++++++++++
# ------------------------------------------------------------------------------
    def stream_data(self, date):
        '''Appends new MLB statcast data for a specified date. Designed to
        be used as a streaming pipline for new daily data during in-season.

        Once the database is built, this method can be used to add new data
        to the RAW and WRK tables. This method should be used while scheduling
        a task to send new daily data through the pipeline during in-season.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        logging.basicConfig(filename=f'{self.parent_path}/logs/stream_data.log',
                            filemode='w',
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%d-%b-%y %H:%M:%S',
                            level=logging.INFO)

        startTime = datetime.now()

        self.dbtype = self.lst_config[0]['Database_System']['db_type']
        self.connect_db()
        try:
            self.engine.connect()
            print(f'Connected to: \nServer: {self.server}\nDatabase: {self.database}')
            logging.info(f'\nConnection Successful \nServer: {self.server}\nDatabase: {self.database}')
        except Exception as e:
            Print('Unable to connect to Server: {self.sever]}')
            logging.exception('Exception Occured')

        try:
            self.df = pd.DataFrame(statcast(start_dt=f'{date}',
                                            end_dt=f'{date}'))
        except Exception as e:
            logging.exception("Exception occurred")

        self.df = self.df.replace({np.nan: None})
        self.dct_parks = dict(zip(self.team_atts['Team'], self.team_atts[f'Ball_Park_{int(self.df.game_year.loc[1])}']))

        if self.df.empty:
            print(f'{date}: NO DATA FOR THIS DATE')
            logging.warning(f'{date}: NO DATA FOR THIS DATE')
        else:
            print('............')
            print(f'|{date}|')
            print('............')
            print(f'Completed: RAW data imported from Baseball Savant')
            logging.info(f'{date}: RAW data imported from Baseball Savant')
            try:
                self.df.drop(['pitcher.1', 'fielder_2.1'],
                             axis=1,
                             inplace=True)
                self.df.to_sql(f'raw_statcast_{int(self.df.game_year.loc[1])}',
                               self.engine,
                               index=False,
                               if_exists='append')
                print(f'Completed: Raw data inserted into DB: STATCAST , TABLE: raw_statcast_{int(self.df.game_year.loc[1])}')
                logging.info(f'{date}: Raw data inserted into DB: STATCAST , TABLE: raw_statcast_{int(self.df.game_year.loc[1])}')
            except Exception as e:
                logging.exception("Exception occurred")
            try:
                self.reorder_columns()
                self.rename_columns()
                self.add_batter_name()
                self.add_player_names()
                self.add_teams()
                self.add_lg_div()
                self.add_ballparks(self.df['Game_Year'][0])
                self.stream_pos_bop_dct(date)
                self.add_batter_pos()
                self.add_batter_bop()
                print(f'Completed: Data transformation')
                logging.info(f'{date}: Data transformation complete')
            except:
                logging.exception("Exception occurred")

            try:
                self.df.to_sql(f'wrk_statcast_{int(self.df.Game_Year.loc[1])}',
                               self.engine,
                               index=False,
                               if_exists='append')
                print(f'Completed: Working data inserted into DB: STATCAST , TABLE: wrk_statcast_{int(self.df.Game_Year.loc[1])}\n')
                logging.info(f'''{date}: Working data inserted into DB: STATCAST , TABLE:
                              wrk_statcast_{int(self.df.Game_Year.loc[1])}\nTime Elapsed: {datetime.now() - startTime}''')
            except Exception as e:
                logging.exception("Exception occurred")


        print(f'--------{date} Complete--------')
        print(f'Total Time Elapsed: {datetime.now() - startTime}')
        logging.info(f'--------{date} Complete--------\nTotal Time Elapsed: {datetime.now() - startTime}')
