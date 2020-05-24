import gdal
import pandas as pd
import numpy as np
import glob, os
import datetime

# defines some useful functions
def load_data(FILEPATH):
    ds = gdal.Open(FILEPATH)
    return ds
    
# Opens the data HDF file and returns as a dataframe
def read_dataset(SUBDATASET_NAME, FILEPATH):
    dataset = load_data(FILEPATH)
    path = ''
    for sub, description in dataset.GetSubDatasets():
        if (description.endswith(SUBDATASET_NAME)):
            path = sub
            break
    if(path == ''):
        print(SUBDATASET_NAME + ' not found')
        return
    subdataset = gdal.Open(path)
    subdataset = subdataset.ReadAsArray()
    subdataset = pd.DataFrame(subdataset)
    return subdataset


# Loads the HDF file, gets the Latitude subdataset and returns the information of the nearest pixel to specified position
def find_latitude_position(CITY_LATITUDE, FILEPATH):
    dataset_to_load = 'Latitude (32-bit floating-point)'
    latitude_dataframe = read_dataset(dataset_to_load, FILEPATH)
    min_diff = 100
    row_number = -1
    column_number = -1
    rows = latitude_dataframe.shape[0]
    columns = latitude_dataframe.shape[1]
    for i in range(rows):
        for j in range(columns):
            lat = latitude_dataframe.iloc[i][j]
            diff = np.abs(lat - CITY_LATITUDE)
            if(diff < min_diff):
                min_diff = diff
                row_number = i
                column_number = j
                found_lat = lat
    if(row_number == -1 or column_number == -1):
        print("Latitude not found. You might have chosen wrong scene")
    return latitude_dataframe, row_number, column_number

# Loads the HDF file, gets the Longitude subdataset and returns the information of the nearest pixel to specified position
def find_longitude_position(CITY_LONGITUDE, LATITUDE_ROW_NUMBER, FILEPATH):
    dataset_to_load = 'Longitude (32-bit floating-point)'
    longitude_dataframe = read_dataset(dataset_to_load, FILEPATH)
    min_diff = 100
    row_number = -1
    column_number = -1
    rows = longitude_dataframe.shape[0]
    columns = longitude_dataframe.shape[1]
    for j in range(columns):
        lon = longitude_dataframe.iloc[LATITUDE_ROW_NUMBER][j]
        diff = np.abs(lon - CITY_LONGITUDE)
        if(diff < min_diff):
            min_diff = diff
            row_number = LATITUDE_ROW_NUMBER
            column_number = j
            found_lon = lon
    if(column_number == -1):
        print("Longitude not found. You might have chosen wrong scene")
    return longitude_dataframe, column_number


# Creates a list of values of the product under observation across all datasets
def create_list(subdataset, CITY_LATITUDE, CITY_LONGITUDE, FILEPATH):
    latitude_dataframe, lat_row, lat_column = find_latitude_position(CITY_LATITUDE, FILEPATH)
    if lat_row == -1:
        return -1
    longitude_dataframe, lon_column = find_longitude_position(CITY_LONGITUDE, lat_row, FILEPATH)
    city_row_number = lat_row
    city_column_number = lon_column
    # 
    row_begin = city_row_number - 1
    row_end = city_row_number + 1
    column_begin = city_column_number - 1
    column_end = city_column_number + 1
    total = 0
    for i in range(row_begin, row_end, 1):
        for j in range(column_begin, column_end, 1):
            pixel_value = find_product_value(i, j, subdataset, FILEPATH)
            if(pixel_value == -9999):
                pixel_value = 0
            total = total + pixel_value
    product_value = total / 9
    return product_value, latitude_dataframe.iloc[lat_row][lat_column], longitude_dataframe.iloc[lat_row][lon_column]


def find_product_value(LATITUDE_ROW_NUMBER, LONGITUDE_COLUMN_NUMBER, SUBDATASET, FILEPATH):
    dataset = read_dataset(SUBDATASET, FILEPATH)
    if(LATITUDE_ROW_NUMBER < 0 or LONGITUDE_COLUMN_NUMBER < 0):
        return -1
    return dataset.iloc[LATITUDE_ROW_NUMBER][LONGITUDE_COLUMN_NUMBER]


def plot(x_axis, y_axis_1, y_axis_2, label1, label2, directory, month):
    figure = plt.figure()
    ax = sns.lineplot(x=date, y=y_axis_1)
    ax = sns.lineplot(x=x_axis, y=y_axis_2)
    figure.legend(labels = [label1, label2])
    plt.show()
    path = (directory + month + '/') + month + label1 + label2 + '.png'
    fig = ax.get_figure()
    fig.savefig(path)
    

def create_averaged_data(observed_parameter):
    observed_parameter = np.array(observed_parameter)
    mean = np.mean(observed_parameter)
    observed_parameter[observed_parameter <= 0] = mean
    return np.mean(observed_parameter)

# utils
def get_julian_date(x):
    return x[10:17]

def get_year(x):
    return jdtodatestd(x).year

def get_month(x):
    return jdtodatestd(x).month

def get_day(x):
    return jdtodatestd(x).day

def jdtodatestd (jdate):
    fmt = '%Y%j'
    datestd = datetime.datetime.strptime(jdate, fmt).date()
    return(datestd)