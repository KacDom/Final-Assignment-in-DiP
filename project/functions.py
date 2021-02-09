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


def time_diff(series, only_hour=False):
    """
    Function for calculating time that elapsed between bus position measurements. Function also checks whether
    time intervals aren't equal to 0.
    :param series: pandas series with information on the date and time of the measurement in a %Y-%m-%d %H:%M:%S format.
    :return: function return a tuple with 2 elements. First is the list of values of time elapsed between bus position
    measurement in hours. Second is a list of indexes on which list of time elapsed between measurements is equal to 0.
    """
    if not only_hour:
        list_of_time_diff = []
        non_empty_indexes = []
        for idx in range(1, len(series)):
            date_time_format = '%Y-%m-%d %H:%M:%S'
            date1 = series.iloc[idx - 1]
            date2 = series.iloc[idx]
            diff = datetime.datetime.strptime(date2, date_time_format) - datetime.datetime.strptime(date1,
                                                                                                    date_time_format)
            list_of_time_diff.append(np.absolute(diff.total_seconds() / 3600))
            non_empty_indexes = [idx for idx in range(len(list_of_time_diff)) if list_of_time_diff[idx] != 0]
            list_of_time_diff = [list_of_time_diff[idx] for idx in non_empty_indexes]
        return list_of_time_diff, non_empty_indexes
    else:
        date1 = series.iloc[0]
        date2 = series.iloc[1]
        hour = int(date2.split(':')[0])
        if hour >= 24:
            date2 = (f'{hour - 24}:{date2.split(":")[1]}:{date2.split(":")[2]}')
        date_time_format = '%H:%M:%S'
        diff = datetime.datetime.strptime(date2, date_time_format) - datetime.datetime.strptime(date1, date_time_format)
        return np.absolute(diff.total_seconds() / 3600)


def calculate_distance(lat, lon):
    """
    Function used to calculated distances in kilometers between spots on earth. It uses latitude and longitude
    information of spots.
    :param lat: pandas series with values of latitude of spots.
    :param lon: pandas series with values of longitude of spots.
    :return: function returns a list of distances between spots in kilometers.
    """
    tuples_of_places = tuple(zip(lat, lon))
    result_distances = [geodesic(tuples_of_places[idx], tuples_of_places[idx + 1]).kilometers for idx in
                        range(len(tuples_of_places) - 1)]
    return result_distances


def speeding(filename, speed_lim=50, from_file=True):
    """
    Function that extracts lines with data only concerning buses that seem to have exceeded speed limit in a given
    period of time.
    :param filename: name of the properly formatted .csv file with the data concerning buses movement.
    :param speed_lim: speed limit in km/h that will be used to determine whether bus was speeding. Must be int. Argument
    is optional
    :return: function returns pandas data frame with information only about buses that are speeding (>50km/h).
    """
    if from_file:
        df_all_data = pd.read_csv(filename, index_col=0)
    else:
        df_all_data = filename
    df_all_data['avg_speed'] = [0 for idx in range(len(df_all_data))]
    set_vehicle = set(df_all_data['VehicleNumber'])
    for veh_num in set_vehicle:
        df = df_all_data[df_all_data['VehicleNumber'] == veh_num]
        indexes = list(df.index)
        time_diff_between_points, non_empty_indexes = time_diff(df['Time'])
        if time_diff_between_points:
            lat_list = df['Lat']
            lon_list = df['Lon']
            time_diff_between_points = [time_diff_between_points[idx] for idx in non_empty_indexes]
            distances = calculate_distance(lat_list, lon_list)
            time_diff_between_points = [time_diff_between_points[idx] for idx in non_empty_indexes]
            distances = [distances[idx] for idx in non_empty_indexes]
            avg_speed_results = list(np.divide(distances, time_diff_between_points))
            avg_speed = [0 for idx in range(len(df))]
            for idx in non_empty_indexes:
                avg_speed[idx + 1] = avg_speed_results[0]
                avg_speed_results = avg_speed_results[1:]
            for idx in indexes:
                df_all_data.loc[df_all_data.index == idx, 'avg_speed'] = avg_speed[0]
                avg_speed = avg_speed[1:]
    return df_all_data[df_all_data['avg_speed'] > speed_lim]


def map_points(df_speeder, map_picture, bbox, fig_name, place_name='Warsaw'):
    """
    Function takes in information on buses that are speeding and puts their location on a map of Warsaw - map was
    downloaded as a PNG file from internet sources. To be able to use the PNG map for this function user has to provide
    coordinated of the edges of the map. If the function is used on default map of Warsaw the coordinates of the edges
    of the map are built in - the default values of 'bbox' argument.
    :param df_speeder: pandas data frame with information only on buses that exceed 50km/h.
    :param map_picture: PNG picture of a map
    :param bbox: coordinates of edges of the map (min longitude, max longitude, min latitude, max latitude). In a form
    tuple with 4 float/int values.
    :param fig_name: title of the plot that will be saved as a result of the function run.
    :param place_name: name of the area on the map that will be used in the title of the created plot. Must be str.
    :return: function creates plot and saves it a .png file.
    """
    longitude = df_speeder['Lon']
    latitude = df_speeder['Lat']
    read_map = plt.imread(map_picture)
    fig, ax = plt.subplots()
    ax.scatter(longitude, latitude, c='r', s=4)
    ax.set_title(f'Buses exceeding 50km/h in {place_name}')
    ax.set_xlim(bbox[0], bbox[1])
    ax.set_ylim(bbox[2], bbox[3])
    ax.imshow(read_map, extent=bbox, aspect='equal')
    plt.savefig(fig_name, dpi=300)


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


def late_buses(df_bus, df_stops):
    """
    Function that checks whether buses are punctual by checking if given bus line from given brigade was close enough
    to specific bus stop at a given time. To say that the bus was punctual it has to be closer than 1 km to the
    destination bus stop at a time that is no longer than 6min from the time the bus is to arrive there. The function
    also assumes that the data on bus positions can be incomplete and in such cases ignores such data.
    :param df_bus: information on bus movement in a form of a pandas data frame
    :param df_stops: information on bus stop in a form of a pandas data frame
    :return: function return a dict with results. There are 2 keys in this dict: 1.'punctual' - information on buses
    that seem to stick to their schedule, 2. 'late' - information on buses that seem to be late.
    """
    buses_punctuality = {'punctual': [], 'late': []}
    for index, row in df_stops.iterrows():
        for key, value in row['bus_schedules'].items():
            if value:
                for sub_key, sub_value in row['bus_schedules'][key].items():
                    df_working = df_bus[(df_bus['Lines'] == key) & (df_bus['Brigade'] == sub_value)]
                    for index_sub, row_sub in df_working.iterrows():
                        t_diff = time_diff(pd.Series([row_sub.Time.split(' ')[1], sub_key]), only_hour=True)
                        if t_diff < 0.1:
                            try:
                                dist = calculate_distance([row_sub.Lat, row.szer_geo], [row_sub.Lon, row.dlug_geo])[0]
                                if dist < 1:
                                    buses_punctuality['punctual'].append(
                                        (key, sub_value, sub_key, f'{round(t_diff * 60, 2)}min', f'{dist}km'))
                                else:
                                    buses_punctuality['late'].append(
                                        (key, sub_value, sub_key, f'{round(t_diff * 60, 2)}min', f'{dist}km'))
                            except:
                                pass
    return buses_punctuality
