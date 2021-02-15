#!/usr/bin/python3
import urllib.request
import os
import re
from geopy.distance import geodesic
import sys
import json
import re
import time
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import multiprocessing

pd.options.mode.chained_assignment = None


def download_bus_data(minutes: int, outfilename: str = 'bus_data.csv',
                      apikey: str = '9a4a37e2-9e5c-46f8-833d-5e1abbe2d85c'):
    """
    Function created for the purpose of downloading data on bus movement in Warsaw. It downloads currently available
    data on https://api.um.warszawa.pl/# website and keeps downloading new data in a loop every minute, which should
    be the refreshment rate of the data on the site. While the function is running in prints messages about how much
    time is left till completion.
    :param minutes: number of datasets downloaded from the page, 1 dataset - 1 minute. Argument type must be int.
    :param outfilename: name of the .csv file with that will be created and filled with downloaded data. The argument is
    optional, default value is 'bus_data.csv'. Argument type must be str.
    :param apikey: apikey used for downloading data from website. Optional argument, default value is
    '9a4a37e2-9e5c-46f8-833d-5e1abbe2d85c'. Argument type must be str.
    :return: function creates a .csv file in the current working directory with downloaded data from website.
    """
    url = f'https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=%20f2e5503e927d-4ad3-9500-4ab9e55deb59&' \
          f'apikey={apikey}&type=1'
    if outfilename in os.listdir():
        sys.exit(f'Such file: "{outfilename}" already exists. Please choose another name.')
    for minute in range(minutes):
        fileobj = urllib.request.urlopen(url)
        string_dict = fileobj.read().decode('UTF-8')
        res = json.loads(string_dict)
        df = pd.DataFrame(res['result'])
        if outfilename not in os.listdir():
            df.to_csv(outfilename)
        else:
            df.to_csv(outfilename, mode='a', header=False)
        print(f'Minutes until downloading data is completed: {minutes - minute}')
        time.sleep(60)
    print('Downloading data has completed!')


def download_bus_stops():
    """
    Function that downloads available data on all bus stops from online data base. The url used to download
    the data is always the same. All the data on the site are place as a whole in one file.
    :return: Function returns information on bus stops in a form of a pandas data frame.
    """
    url = 'https://api.um.warszawa.pl/api/action/dbstore_get/?id=ab75c33d-3a26-4342-b36a-6e5fef0a3ac3&apikey=9a4a37e2-9e5c-46f8-833d-5e1abbe2d85c'
    fileobj = urllib.request.urlopen(url)
    string_dict = fileobj.read().decode('UTF-8')
    res = json.loads(string_dict)
    list_of_dicts = []
    for dict in res['result']:
        temp_dict = {}
        for dict_2 in dict['values']:
            temp_dict[dict_2['key']] = dict_2['value']
        list_of_dicts.append(temp_dict)
    df = pd.DataFrame(list_of_dicts)
    return df


def what_bus_line(url):
    """
    Function that checks what bus line stop at given bus stop.
    :param url: Url from where the data is to be downloaded or list of urls.
    :return: returns a list of bus lines, or a dict of bus lines if the "url" parameter was a list.
    """
    if type(url) == list:
        dict_with_all = {}
        for sub_list in url:
            fileobj = urllib.request.urlopen(sub_list)
            string_dict = fileobj.read().decode('UTF-8')
            res = json.loads(string_dict)
            list_of_dicts = []
            for dictionary in res['result']:
                temp_dict = {}
                for dict_2 in dictionary['values']:
                    temp_dict[dict_2['key']] = dict_2['value']
                list_of_dicts.append(temp_dict)
            dict_with_all[re.findall('line=(.*)&', sub_list)[0]] = list_of_dicts
        return dict_with_all
    fileobj = urllib.request.urlopen(url)
    string_dict = fileobj.read().decode('UTF-8')
    res = json.loads(string_dict)
    list_of_lines = []
    for dict in res['result']:
        for dict_2 in dict['values']:
            list_of_lines.append(dict_2['value'])
    if not list_of_lines:
        return 'nan'
    return list_of_lines


def generate_urls(stop_id, stop_nr, specific_line=None):
    """
    Function that generates Url that will be used to download either information on what bus lines stop at each bus
    stops or schedule for each bus line.
    :param stop_id: ID of the bus stop in question.
    :param stop_nr: Bus stop number of the bus stop in question.
    :param specific_line: Bus line if we want to download bus schedule for a specific bus line.
    :return: Function returns a Url in a form of string or a list of Urls.
    """
    if not specific_line:
        return f'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId={stop_id}&busstopNr={stop_nr}&apikey=9a4a37e2-9e5c-46f8-833d-5e1abbe2d85c'
    else:
        return [
            f"https://api.um.warszawa.pl/api/action/dbtimetable_get?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238&busstopId={stop_id}&busstopNr={stop_nr}&line={line}&apikey=9a4a37e2-9e5c-46f8-833d-5e1abbe2d85c"
            for line in specific_line]


def add_bus_lines(df):
    """
    Function that checks what bus lines stop and each bus stop and downloads bus schedule for each line.
    :param df: takes in a pandas data frame with information on bus stops.
    :return: function returns pandas data frame with information on what bus lines stop at each bus stop and bus line
    schedules.
    """
    df_apply = df.apply(lambda row: generate_urls(str(row['zespol']), str(row['slupek'])), axis=1)
    list_of_web_adresses = list(df_apply)
    results = multiprocessing.Pool().map(what_bus_line, list_of_web_adresses)
    df['bus_lines'] = results
    df = df[df['bus_lines'] != 'nan']
    df_apply = df.apply(
        lambda row: generate_urls(str(row['zespol']), str(row['slupek']), specific_line=row['bus_lines']), axis=1)
    list_of_web_adresses = list(df_apply)
    results = multiprocessing.Pool().map(what_bus_line, list_of_web_adresses)
    df['bus_schedules'] = results
    for index, row in df.iterrows():
        new_dict = {}
        for key in row['bus_schedules']:
            new_dict[key] = {}
            for sub_dict in row['bus_schedules'][key]:
                new_dict[key][sub_dict['czas']] = sub_dict['brygada']
        df['bus_schedules'][index] = new_dict
    return df

