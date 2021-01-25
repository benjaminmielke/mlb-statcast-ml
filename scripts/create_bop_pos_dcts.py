import pandas as pd
import datetime
import json
import yaml


def create_dct_bop():
    '''
    Create dictionary json files for each year containing which position in the
    batting order a player was playing for each date in their career.

    This function uses a MLB PlayerID Map and baseball-reference.com game-logs
    to create yearly dictionaries containing which position in the batting order
    a player was for each game in their career. The function scarpes data from
    the baseball-reference.com game logs for each player then parses and
    converts the data into the form neccessary to use in the Statcast Databse.

    Example of an entry into a dictionary:
    {"Jose Abreu": {"2020-07-24": "3", "2020-07-25": "3" ...

    Args:
        No arguments.

    Returns:
        list: a list of each years dictionary

    '''
    with open('config.yaml', 'r') as yamlfile:
        lst_config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    parent_path = lst_config[1]['Paths']['parent_path']

    playeridmap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV')

    lst_year = ['2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013', '2012', '2011', '2010', '2009', '2008']
    df_2020 = pd.DataFrame()
    df_2019 = pd.DataFrame()
    df_2018 = pd.DataFrame()
    df_2017 = pd.DataFrame()
    df_2016 = pd.DataFrame()
    df_2015 = pd.DataFrame()
    df_2014 = pd.DataFrame()
    df_2013 = pd.DataFrame()
    df_2012 = pd.DataFrame()
    df_2011 = pd.DataFrame()
    df_2010 = pd.DataFrame()
    df_2009 = pd.DataFrame()
    df_2008 = pd.DataFrame()
    dct_bop_2020 = {}
    dct_bop_2019 = {}
    dct_bop_2018 = {}
    dct_bop_2017 = {}
    dct_bop_2016 = {}
    dct_bop_2015 = {}
    dct_bop_2014 = {}
    dct_bop_2013 = {}
    dct_bop_2012 = {}
    dct_bop_2011 = {}
    dct_bop_2010 = {}
    dct_bop_2009 = {}
    dct_bop_2008 = {}
    lst_df_year = [df_2020, df_2019, df_2018, df_2017, df_2016, df_2015, df_2014, df_2013, df_2012, df_2011, df_2010, df_2009, df_2008]
    lst_dct_bop_year = [dct_bop_2020, dct_bop_2019, dct_bop_2018, dct_bop_2017, dct_bop_2016, dct_bop_2015, dct_bop_2014, dct_bop_2013, dct_bop_2012, dct_bop_2011, dct_bop_2010, dct_bop_2009, dct_bop_2008]

    playeridmap_hitters = playeridmap.drop(playeridmap[playeridmap['POS'].str.contains('P')].index)
    playeridmap_hitters = playeridmap_hitters.reset_index(drop=True)
    dct_brefid_name = dict(zip(playeridmap_hitters.BREFID, playeridmap_hitters.PLAYERNAME))
    lst_brefid = playeridmap_hitters['BREFID']
    for p in range(0, len(lst_brefid)):
        for y in range(0, len(lst_year)):
            try:
                lst_df_year[y] = pd.read_html(f'https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={lst_year[y]}')[4][['Date', 'BOP', 'Pos']]
            except:
                print(f'{dct_brefid_name.get(lst_brefid[p])}:{lst_year[y]}: Table Not Found: https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={lst_year[y]}')
                continue
            lst_df_year[y]['Date'] = lst_df_year[y].Date.map(str) + " " + lst_year[y]
            lst_df_year[y].drop(lst_df_year[y][lst_df_year[y]['Date'].str.contains('Date')].index, inplace=True)
            lst_df_year[y].drop(lst_df_year[y][lst_df_year[y]['Date'].str.contains('nan')].index, inplace=True)
            lst_df_year[y] = lst_df_year[y].reset_index(drop=True)

            lst_df_year[y]['Date'] = lst_df_year[y]['Date'].str.replace('(1)', '', regex=False)
            lst_df_year[y]['Date'] = lst_df_year[y]['Date'].str.replace('(2)', '', regex=False)
            lst_df_year[y]['Date'] = lst_df_year[y]['Date'].str.replace('susp', '', regex=False)

            for i in range(0, len(lst_df_year[y]['Date'])):
                lst_df_year[y]['Date'][i] = datetime.datetime.strptime(lst_df_year[y]['Date'][i], '%b %d %Y').strftime('%Y-%m-%d')
            lst_dct_bop_year[y].update({dct_brefid_name.get(lst_brefid[p]): dict(zip(lst_df_year[y].Date, lst_df_year[y].BOP))})
            lst_df_year[y] = []

    json.dump(lst_dct_bop_year[y], open(f'{parent_path}dicts/dct_bop_{lst_year[y]}.txt', 'w'))

    print('BOP Dictionaries Completed')

    return lst_dct_bop_year


def create_dct_pos():
    '''
    Create dictionary json files for each year containing which position the
    player was playing for each date in their career.

    This function uses a MLB PlayerID Map and baseball-reference.com game-logs
    to create yearly dictionaries containing which position the player was
    playing for each game in their career. The function scarpes data from
    the baseball-reference.com game logs for each player then parses and
    converts the data into the form neccessary to use in the Statcast Databse.

    Example of an entry into a dictionary:
    {"Jose Abreu": {"2020-07-24": "1B", "2020-07-25": "1B" ...

    Args:
        No arguments.

    Returns:
        list: a list of each years dictionary

    '''

    with open('config.yaml', 'r') as yamlfile:
        lst_config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    parent_path = lst_config[1]['Paths']['parent_path']

    playeridmap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV')

    lst_year = ['2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013', '2012', '2011', '2010', '2009', '2008']
    df_2020 = pd.DataFrame()
    df_2019 = pd.DataFrame()
    df_2018 = pd.DataFrame()
    df_2017 = pd.DataFrame()
    df_2016 = pd.DataFrame()
    df_2015 = pd.DataFrame()
    df_2014 = pd.DataFrame()
    df_2013 = pd.DataFrame()
    df_2012 = pd.DataFrame()
    df_2011 = pd.DataFrame()
    df_2010 = pd.DataFrame()
    df_2009 = pd.DataFrame()
    df_2008 = pd.DataFrame()
    dct_pos_2020 = {}
    dct_pos_2019 = {}
    dct_pos_2018 = {}
    dct_pos_2017 = {}
    dct_pos_2016 = {}
    dct_pos_2015 = {}
    dct_pos_2014 = {}
    dct_pos_2013 = {}
    dct_pos_2012 = {}
    dct_pos_2011 = {}
    dct_pos_2010 = {}
    dct_pos_2009 = {}
    dct_pos_2008 = {}
    lst_df_year = [df_2020, df_2019, df_2018, df_2017, df_2016, df_2015, df_2014, df_2013, df_2012, df_2011, df_2010, df_2009, df_2008]
    lst_dct_pos_year = [dct_pos_2020, dct_pos_2019, dct_pos_2018, dct_pos_2017, dct_pos_2016, dct_pos_2015, dct_pos_2014, dct_pos_2013, dct_pos_2012, dct_pos_2011, dct_pos_2010, dct_pos_2009, dct_pos_2008]

    playeridmap_hitters = playeridmap.drop(playeridmap[playeridmap['POS'].str.contains('P')].index)
    playeridmap_hitters = playeridmap_hitters.reset_index(drop=True)
    dct_brefid_name = dict(zip(playeridmap_hitters.BREFID, playeridmap_hitters.PLAYERNAME))
    lst_brefid = playeridmap_hitters['BREFID']
    for p in range(0, len(lst_brefid)):
        for y in range(0, len(lst_year)):
            try:
                lst_df_year[y] = pd.read_html(f'https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={lst_year[y]}')[4][['Date', 'BOP', 'Pos']]
            except:
                print(f'{dct_brefid_name.get(lst_brefid[p])}:{lst_year[y]}: Table Not Found: https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={lst_year[y]}')
                continue
            lst_df_year[y]['Date'] = lst_df_year[y].Date.map(str) + " " + lst_year[y]
            lst_df_year[y].drop(lst_df_year[y][lst_df_year[y]['Date'].str.contains('Date')].index, inplace = True)
            lst_df_year[y].drop(lst_df_year[y][lst_df_year[y]['Date'].str.contains('nan')].index, inplace = True)
            lst_df_year[y] = lst_df_year[y].reset_index(drop=True)

            lst_df_year[y]['Date'] = lst_df_year[y]['Date'].str.replace('(1)', '', regex=False)
            lst_df_year[y]['Date'] = lst_df_year[y]['Date'].str.replace('(2)', '', regex=False)
            lst_df_year[y]['Date'] = lst_df_year[y]['Date'].str.replace('susp', '', regex=False)

            for i in range(0, len(lst_df_year[y]['Date'])):
                lst_df_year[y].loc[i, 'Date'] = datetime.datetime.strptime(lst_df_year[y].loc[i, 'Date'], '%b %d %Y').strftime('%Y-%m-%d')
            lst_dct_pos_year[y].update({dct_brefid_name.get(lst_brefid[p]): dict(zip(lst_df_year[y]['Date'], lst_df_year[y]['Pos']))})
            lst_df_year[y] = []

    json.dump(lst_dct_pos_year[y], open(f'{parent_path}dicts/dct_pos_{lst_year[y]}_test.txt', 'w'))

    print('Pos Dictionaries Completed')

    return lst_dct_pos_year
