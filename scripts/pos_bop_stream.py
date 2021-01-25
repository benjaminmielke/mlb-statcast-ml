import pandas as pd
import datetime
import yaml
import ujson


def create_dct_bop():
    '''
    Create dictionary json files for each year containing which position in the
    batting order a player was playing for each date in their career.

    This function uses a MLB PlayerID Map and baseball-reference.com game-logs
    to create yearly dictionaries containing which position in the batting order
    a player was for each game in their career. The function scrapes data from
    the baseball-reference.com game logs for each player then parses and
    converts the data into the form neccessary to use in a Statcast Databse.

    Example of an entry into a dictionary:
    {"Jose Abreu": {"2020-07-24": "3", "2020-07-25": "3" ...

    Args:
        No arguments.

    Returns:
        list: a list of each years dictionary

    '''


def stream_pos_bop_dct(date):
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
    parent_path = lst_config[1]['Paths']['parent_path']

    dct_pos_2021 = {}
    dct_bop_2021 = {}

    year = date[:4]
    filename_pos = f'{parent_path}/dicts/dct_pos_2021_test.txt'
    filename_bop = f'{parent_path}/dicts/dct_bop_2021_test.txt'

    playeridmap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV', nrows=200)

    try:
        dct_pos_2021 = ujson.load(open(filename_pos))
    except ValueError:
        print('The Pos text file is empty.')
    try:
        dct_bop_2021 = ujson.load(open(filename_bop))
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
        df.loc[0, 'Date'] = datetime.datetime.strptime(df.loc[0, 'Date'], '%b %d %Y').strftime('%Y-%m-%d')

        if df.loc[0, 'Date'] == date:
            if dct_brefid_name.get(lst_brefid[p]) in dct_pos_2021:
                dct_pos_2021.get(dct_brefid_name.get(lst_brefid[p])).update(dict(zip(df['Date'], df['Pos'])))
                print(f'Updated Pos: {dct_brefid_name.get(lst_brefid[p])}: entry updated for {date}')
            else:
                dct_pos_2021.update({dct_brefid_name.get(lst_brefid[p]): dict(zip(df['Date'], df['Pos']))})
                print(f'Inserted Pos: {dct_brefid_name.get(lst_brefid[p])}: new entry inserted for {date}')

            if dct_brefid_name.get(lst_brefid[p]) in dct_bop_2021:
                dct_bop_2021.get(dct_brefid_name.get(lst_brefid[p])).update(dict(zip(df['Date'], df['BOP'])))
                print(f'Updated BOP: {dct_brefid_name.get(lst_brefid[p])}: entry updated for {date}')
            else:
                dct_bop_2021.update({dct_brefid_name.get(lst_brefid[p]): dict(zip(df['Date'], df['BOP']))})
                print(f'Inserted BOP: {dct_brefid_name.get(lst_brefid[p])}: new entry inserted for {date}')
        else:
            print(f'DNP: {dct_brefid_name.get(lst_brefid[p])}: Did not play {date}')

    ujson.dump(dct_pos_2021, open(filename_pos, 'w'))
    ujson.dump(dct_bop_2021, open(filename_bop, 'w'))

    print(f'Pos Dictionary Completed for {date}')
    print(f'BOP Dictionary Completed for {date}')

    return dct_pos_2021, dct_bop_2021
