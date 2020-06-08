# standard libraries
import glob, os
import datetime

# third party
import gdal
import pandas as pd
import numpy as np
from tqdm import tqdm

# local
from data_proccesing_utils import (
    load_data,
    read_dataset,
    find_latitude_position,
    find_longitude_position,
    create_list,
    find_product_value,
    get_julian_date,
    get_year,
    get_month,
    get_day,
    jdtodatestd
)

def main():

    try:
        os.chdir("/Users/j/repos/nasa_laas_daac/src")
    except:
        print os.path
        pass

    # change to data directory
    os.chdir("../data/processed")

    points = pd.read_csv("queretaro_points.csv")

    # change to data directory
    os.chdir("../raw")

    metrics = []
    master = []
    start = 0
    stop = 10
    for index, row in tqdm(points.iterrows()):

        try:
            # if row["id"] < start:
            #     continue
            # if row["id"] == stop:
            #     break

            values_land_and_ocean = []
            ruta = []
            ids = []
            file_names = []
            point_lat = []
            point_lon = []
            latitude = []
            longitude = []
            errors = []

            # dataset to extract
            SUBDATASET_NAME = "Optical_Depth_Land_And_Ocean (16-bit integer)"

            for index, file in enumerate(glob.glob('*.hdf')):
                # if index == 100:
                #     break

                try:
                    FILEPATH = file
                    land_and_ocean, lat, lon = create_list(SUBDATASET_NAME, row["lat"], row["lon"], FILEPATH)
                    # print("Data collected for: " + str(lat) + " N and " + str(lon) + " E.")
                    file_names.append(file)
                    point_lat.append(row["lat"])
                    point_lon.append(row["lon"])
                    values_land_and_ocean.append(land_and_ocean)
                    latitude.append(lat)
                    longitude.append(lon)
                    ruta.append(row["ruta"])
                    ids.append(row["id"])
                except:
                    errors.append(file)

            # build dataframe
            final_list = pd.DataFrame()
            final_list['File Name'] = pd.Series(file_names)
            final_list['ruta'] = pd.Series(ruta)
            final_list['id'] = pd.Series(ids)
            # add latitude
            final_list['Latitude'] = pd.Series(latitude)
            final_list['Longitude'] = pd.Series(longitude)
            # extract year, month and day.
            final_list["julian_date"] = final_list["File Name"].apply(get_julian_date)
            final_list["year"] = final_list["julian_date"].apply(get_year)
            final_list["month"] = final_list["julian_date"].apply(get_month)
            final_list["year_month"] = final_list["year"].astype(str) + final_list["month"].astype(str)
            final_list["day"] = final_list["julian_date"].apply(get_day)
            final_list['land_and_ocean'] = pd.Series(values_land_and_ocean)

            # drop rows with value 0.
            final_list = final_list.query("land_and_ocean != 0")
            try:
                os.mkdir("../processed/{}".format(row["ruta"]))
            except:
                pass
            final_list.to_csv("../processed/{}/{}_{}.csv".format(row["ruta"]  ,row["ruta"], row["id"]), index=False)
            master += final_list.to_dict('records')

            info = dict(row)
            info["num_obervations"] = len(final_list)
            info["erros"] = len(errors)
            metrics.append(info)
            pd.DataFrame(metrics).to_csv("../processed/metrics.csv", index=False)
            pd.DataFrame(master).to_csv("../processed/master.csv", index=False)
        except Exception as e:
            print "Errors in row {}: {}".format(row["id"], e)

if __name__ == "__main__":
    main()
