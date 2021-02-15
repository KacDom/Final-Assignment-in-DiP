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
