with open('config.yaml', 'r') as yamlfile:
    lst_config = yaml.load(yamlfile, Loader=yaml.FullLoader)
parent_path = lst_config[1]['Paths']['parent_path']

dct_bop_2021 = {}
year = '2020'
filename = f'{parent_path}dicts/dct_bop_2021_test'

playeridmap = pd.read_csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV', nrows=20)

# infile = open(filename, 'rb')
# dct_bop_2021 = pickle.load(infile)
# infile.close()
dct_bop_2021 = json.load(open(f'dct_bop_{year}.txt'))

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

        df.drop(df.index[0:-2], inplace=True)
        df.drop(df.index[-1], inplace=True)
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
