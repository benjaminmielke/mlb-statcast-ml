import pandas as pd
import datetime
import pickle
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

    dct_bop_2021 = {}
    year = '2020'
    filename = f'{parent_path}dicts/dct_bop_2021_test'

    playeridmap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV', nrows=20)

    infile = open(filename, 'rb')
    dct_bop_2021 = pickle.load(infile)
    infile.close()

    print('dct/n')
    print(dct_bop_2021)

    playeridmap_hitters = playeridmap.drop(playeridmap[playeridmap.POS.str.contains('P')].index)
    playeridmap_hitters = playeridmap_hitters.reset_index(drop=True)
    dct_brefid_name = dict(zip(playeridmap_hitters.BREFID, playeridmap_hitters.PLAYERNAME))
    lst_brefid = playeridmap_hitters.BREFID
    for p in range(0, len(lst_brefid)):
            try:
                df = pd.read_html(f'https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={year}')[4][['Date', 'BOP', 'Pos']]
            except:
                print(f'{dct_brefid_name.get(lst_brefid[p])}:{year}: Table Not Found: https://www.baseball-reference.com/players/gl.fcgi?id={lst_brefid[p]}&t=b&year={year}')
                continue

            df.drop(df.index[0:-3], inplace=True)
            df.drop(df.index[-2:], inplace=True)
            print(df)

            df['Date'] = df.Date.map(str) + " " + year
            df.drop(df[df['Date'].str.contains('Date')].index, inplace=True)
            df.drop(df[df['Date'].str.contains('nan')].index, inplace=True)
            df = df.reset_index(drop=True)

            df['Date'] = df['Date'].str.replace('(1)', '', regex=False)
            df['Date'] = df['Date'].str.replace('(2)', '', regex=False)
            df['Date'] = df['Date'].str.replace('susp', '', regex=False)

            for i in range(0, len(df['Date'])):
                df['Date'][i] = datetime.datetime.strptime(df['Date'][i], '%b %d %Y').strftime('%Y-%m-%d')
            dct_bop_2021.update({dct_brefid_name.get(lst_brefid[p]): dict(zip(df.Date, df.BOP))})
            df = []

    print('dct/n')
    print(dct_bop_2021)

    outfile = open(filename, 'wb')
    pickle.dump(dct_bop_2021, outfile)
    outfile.close()

    print('BOP Dictionary Completed')

    return dct_bop_2021
