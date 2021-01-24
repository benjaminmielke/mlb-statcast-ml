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
import json
import yaml


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

        self.parent_path = self.lst_config[1]['Paths']['parent_path']
        self.playerMap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV',
                                     usecols=['MLBID', 'MLBNAME', 'BREFID', 'POS', 'PLAYERNAME'],
                                     dtype={'MLBID': 'category', 'MLBNAME': 'category'},
                                     skiprows=[2604+1])
        self.dct_playerIDs = dict(zip(self.playerMap.MLBID.astype(float), self.playerMap.MLBNAME))
        self.team_atts = pd.read_csv(f'{self.parent_path}csv/team_atts.csv')
        self.dct_team_league = dict(zip(self.team_atts.Team, self.team_atts.League))
        self.dct_team_division = dict(zip(self.team_atts.Team, self.team_atts.Division))


        print('To Build an initial Database, call build_db()')
        print('To append date(s) into Database, call stream_data("yyyy-mm-dd")')

    def get_variables(self):
        '''A method for the button in the prompt window to call that retreives the entry
        values for the database information.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        self.dbtype = self.entry_a.get()
        self.driver = self.entry_b.get()
        self.server = self.entry_c.get()
        self.database = self.entry_d.get()
        self.connect()
        self.window.destroy()

    def prompt_window(self):
        '''Opens a prompt window for the user to enter the SQL database information that
        will be used for the data to be entered into.

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
        self.window.title('PLEASE ENTER MSSQL DATABASE INFO')

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

        label_m = tk.Label(master=frame_m,
                           text='Welcome to the MLB StatCast Database Builder!',
                           fg='white',
                           pady=5,
                           bg='#0E4B95')
        label_m.pack()

        label_a = tk.Label(master=frame_a,
                           text='Enter Database Type:',
                           relief=tk.SUNKEN,
                           padx=2)
        label_a.pack(side='left')
        self.entry_a = tk.Entry(master=frame_a,
                                width='50',
                                fg='#9F0808')
        self.entry_a.pack(side='right')
        self.entry_a.insert(0, 'mssql+pyodbc')

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

        button_get = tk.Button(text='CONNECT and BUILD',
                               command=self.get_variables,
                               bg='#9F0808',
                               fg='white')
        button_get.pack(pady=3)

        self.window.eval('tk::PlaceWindow . center')
        self.window.mainloop()

    def finished_window(self):
        pass


    def connect(self):
        '''Connects and creates an engine for the SQL Database specified
        by the user.

        Args:
            No arguments

        Returns:
            Does not return a parameter

        Raises:
            No exceptions
        '''
        params = urllib.parse.quote_plus(f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database}")
        self.engine = create_engine(f"{self.dbtype}:///?odbc_connect=%s" % params)

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
        with open(f'{self.parent_path}lists/lst_reorder_cols.txt', 'r') as filehandler:
            lst_reorder_columns = [line[:-1] for line in filehandler]

        self.df = self.df[lst_reorder_columns]

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
        with open(f'{self.parent_path}lists/lst_rename_cols.txt', 'r') as filehandler:
            lst_rename_columns = [line[:-1] for line in filehandler]

        self.df.columns = lst_rename_columns

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

        self.df.insert(self.df.columns.get_loc('Bat_Score'),
                       'Bat_Team',
                       lst_bat_team)
        self.df.insert(self.df.columns.get_loc('Bat_Score'),
                       'Fld_Team',
                       lst_fld_team)

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

        self.df.insert(self.df.columns.get_loc('Bat_Score'),
                       'Bat_Team_League',
                       lst_bat_league)
        self.df.insert(self.df.columns.get_loc('Bat_Score'),
                       'Bat_Team_Division',
                       lst_bat_division)
        self.df.insert(self.df.columns.get_loc('Bat_Score'),
                       'Fld_Team_League',
                       lst_fld_league)
        self.df.insert(self.df.columns.get_loc('Bat_Score'),
                       'Fld_Team_Division',
                       lst_fld_division)

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


        logging.basicConfig(filename=f'{self.parent_path}logs/build_db.log',
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
        lst_month = ['03', '06']
        lst_day_31 = ['01', '31']
        lst_day_30 = ['01','30']

#         lst_year =  list(reversed([str(i) for i in range(2008,2021)]))
#         lst_month = ['0'+str(i) for i in range(3,10)] + [str(i) for i in range(10,12)]
#         lst_day_31 = ['0'+str(i) for i in range(1,10)] + [str(i) for i in range(10,32)]
#         lst_day_30 = ['0'+str(i) for i in range(1,10)] + [str(i) for i in range(10,31)]



        for y in range(0, len(lst_year)):
            self.dct_pos = json.load(open(f'{self.parent_path}dicts/dct_bop_{lst_year[y]}.txt'))
            self.dct_bop = json.load(open(f'{self.parent_path}dicts/dct_pos_{lst_year[y]}.txt'))
            self.dct_parks = dict(zip(self.team_atts.Team, self.team_atts[f'Ball_Park_{lst_year[y]}']))

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
                            self.df.drop(['pitcher.1', 'fielder_2.1'],
                                         axis=1,
                                         inplace=True)
                            self.df.to_sql(f'testing_RAW_{lst_year[y]}',
                                           self.engine,
                                           index=False,
                                           if_exists='append')
                            print(f'Completed: Raw data inserted into DB: STATCAST , TABLE: testing_RAW_{lst_year[y]}')
                            logging.info(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Raw data inserted into DB: STATCAST , TABLE: testing_RAW_{lst_year[y]}')
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
                            print(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Data transformation complete')
                            logging.info(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Data transformation complete')
                        except Exception as e:
                            logging.exception('Exception Occured')

                        try:
                            self.df.to_sql(f'testing_WRK_{lst_year[y]}',
                                           self.engine,
                                           index=False,
                                           if_exists='append')
                            print(f'Completed: Working data inserted into DB: STATCAST , TABLE: testing_RAW_{lst_year[y]}')
                            print(f'Time Elapsed: {datetime.now() - startTime}\n')
                            logging.info(f'{lst_year[y]}-{lst_month[m]}-{lst_day[d]}: Working data inserted into DB: STATCAST , TABLE: testing_WRK_{lst_year[y]}\nTime Elapsed: {datetime.now() - startTime}')
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
        logging.basicConfig(filename=f'{self.parent_path}logs/stream_data.log',
                            filemode='w',
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%d-%b-%y %H:%M:%S',
                            level=logging.INFO)

        startTime = datetime.now()

        self.dbtype = self.lst_config[0]['Database']['db_type']
        self.driver = self.lst_config[0]['Database']['driver']
        self.server = self.lst_config[0]['Database']['server']
        self.database = self.lst_config[0]['Database']['database']

        params = urllib.parse.quote_plus(f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database}")
        self.engine = create_engine(f"{self.dbtype}:///?odbc_connect=%s" % params)
        print(f'Connected to: \nServer: {self.server}\nDatabase: {self.database}')
        logging.info(f'\nConnection Successful \nServer: {self.server}\nDatabase: {self.database}')

        try:
            self.df = pd.DataFrame(statcast(start_dt=f'{date}',
                                            end_dt=f'{date}'))
        except Exception as e:
            logging.exception("Exception occurred")

        self.df = self.df.replace({np.nan: None})
        self.dct_parks = dict(zip(self.team_atts.Team, self.team_atts[f'Ball_Park_{int(self.df.game_year.loc[1])}']))

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
                self.df.to_sql(f'testing_RAW_{int(self.df.game_year.loc[1])}',
                               self.engine,
                               index=False,
                               if_exists='append')
                print(f'Completed: Raw data inserted into DB: STATCAST , TABLE: testing_RAW_{int(self.df.game_year.loc[1])}')
                logging.info(f'{date}: Raw data inserted into DB: STATCAST , TABLE: testing_RAW_{int(self.df.game_year.loc[1])}')
            except Exception as e:
                logging.exception("Exception occurred")
            try:
                self.reorder_columns()
                self.rename_columns()
                self.add_batter_name()
                self.add_player_names()
                self.add_teams()
                self.add_lg_div()
                self.add_ballparks(self.df['Game_Date'][0])
                print(f'Completed: Data transformation')
                logging.info(f'{date}: Data transformation complete')
            except:
                logging.exception("Exception occurred")

            try:
                self.df.to_sql(f'testing_WRK_{int(self.df.Game_Year.loc[1])}',
                               self.engine,
                               index=False,
                               if_exists='append')
                print(f'Completed: Working data inserted into DB: STATCAST , TABLE: testing_WRK_{int(self.df.Game_Year.loc[1])}\n')
                logging.info(f'''{date}: Working data inserted into DB: STATCAST , TABLE:
                              testing_WRK_{int(self.df.Game_Year.loc[1])}\nTime Elapsed: {datetime.now() - startTime}''')
            except Exception as e:
                logging.exception("Exception occurred")


        print(f'--------{date} Complete--------')
        print(f'Total Time Elapsed: {datetime.now() - startTime}')
        logging.info(f'--------{date} Complete--------\nTotal Time Elapsed: {datetime.now() - startTime}')
