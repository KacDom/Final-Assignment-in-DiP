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
