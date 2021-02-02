from selenium import webdriver
import pandas as pd
import json
import yaml

# need to add docstring
def create_ss_dcts():
    lst_year = ['2015', '2016', '2017', '2018', '2019', '2020']

    with open('C:/Users/okiem/github/mlb-statcast-ml/scripts/config.yaml', 'r') as yamlfile:
        lst_config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    parent_path = lst_config[3]['Paths']['parent_path']

    # need to add docstring
    def rearrange_name(s, word, pos):
        split = s.split()
        split.insert(pos, split.pop(split.index(word)))
        new_string = ' '.join(split).replace(',', '')
        if len(new_string.split()) == 3:
            new_string = new_string.replace(new_string.split()[2], '').rstrip()
            return new_string
        else:
            return new_string

    for y in range(0, len(lst_year)):
        driver_2020 = webdriver.Chrome('C:/Users/okiem/Folders/Downloads/chromedriver_win32/chromedriver.exe')
        driver_2020.get(f'https://baseballsavant.mlb.com/leaderboard/sprint_speed?year=2020&position=&team=&min=0')
        html = driver_2020.page_source

        tables = pd.read_html(html)
        df = tables[0]
        driver_2020.close()

        lst_new_names = [rearrange_name(x, x.split()[1], 0) for x in df['Player']]
        df['Player'] = lst_new_names

        dct = dict(zip(df['Player'], df['Sprint Speed (ft / sec)']))

        json.dump(dct, open(f'{parent_path}/dicts/dct_ss_2020.txt', 'w'))
