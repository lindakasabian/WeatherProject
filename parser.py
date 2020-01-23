import json
import csv
import glob
import os
from dct_station import dct_stations


def fill_emptiness(dct, variable):
    try:
        if dct[variable]:
            pass
    except KeyError:
        dct[variable] = '0.0'


def degree_to_direction(num):
    try:
        val = int(int(num) / 45)
        arr = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        return arr[(val % 8)]
    except ValueError:
        return None


def wwo_read(file):
    if os.path.exists(f"{file}.csv"):
        os.rename(f"{file}.csv", f"wwo_data/{file}.csv")
    with open(f"wwo_data/{file}.csv") as csvfile:
        readedcsv = csv.reader(csvfile, delimiter=',')
        lst = []
        dct = []
        for row in readedcsv:
            wind_dir = row[-2]
            wind_speed = row[-1]
            wind_dir = degree_to_direction(wind_dir)
            date_wwo = row[0]
            dct.append(date_wwo)
            dct.append(wind_dir)
            dct.append(wind_speed)
            dct.append(file)
            lst.append(dct)
        return lst


def wwo_read_case_no_noaa(file):
    with open(f"wwo_data/{file}.csv") as csvfile:
        readedcsv = csv.reader(csvfile, delimiter=',')
        lst = []
        dct = []
        for row in readedcsv:
            wind_dir = row[-2]
            wind_speed = row[-1]
            wind_dir = degree_to_direction(wind_dir)
            date_wwo = row[0]
            max_t = row[1]
            min_t = row[2]
            try:
                avg_t = (int(max_t) + int(min_t))/2
            except ValueError:
                avg_t = '0.0'
            prec = row[-6]
            dct.append(date_wwo)
            dct.append(wind_dir)
            dct.append(wind_speed)
            dct.append(avg_t)
            dct.append(prec)
            lst.append(dct)
        return lst


def parse_data_case_no_noaa():
    city_handler_2 = {}
    lst_handler = []
    cntr = 0
    for city, station in dct_stations.items():
        lst = wwo_read_case_no_noaa(city)[cntr][5:]
        lst_handler.append(lst)
        cntr += 1

    with open("output_final.json", "w") as output_final:
        output_final.write("[")
        lst_handler = lst_handler
        for i, items in enumerate(lst_handler):
            if i % 5 == 0:
                city_handler_2["DATE"] = items[0]
            if i % 5 == 1:
                city_handler_2["wind_direction"] = items[1]
            if i % 5 == 2:
                city_handler_2["wind_speed"] = items[2]
            if i % 5 == 3:
                city_handler_2["TAVG"] = items[3]
            if i % 5 == 4:
                city_handler_2["PRCP"] = items[4]
            value = city_handler_2
            output_final.write(str(value).replace("'", "\"") + ",")

    with open("output_final.json", 'rb+') as filehandle:
        filehandle.seek(-1, os.SEEK_END)
        filehandle.truncate()
        filehandle.write(b"]")

    with open("output_final.json", "r") as file:
        list = json.load(file)
        list = list[4::4]
        print(list)

parse_data_case_no_noaa()


def parse_data(data):
    with open(data, "r") as noaa_file:
        noaa_raw = json.load(noaa_file)
        noaa_json = noaa_raw

    city_handler = {}
    for city, station in dct_stations.items():
        city_handler[station] = wwo_read(city)

    with open("output_final.json", "w") as output_final:
        output_final.write("[")
        for key, value in city_handler.items():
            for elt in noaa_json:
                if elt["STATION"] == key:
                    indx = value[0].index(elt["DATE"])
                    if value[0][indx] == elt["DATE"]:
                        elt["wind_direction"] = value[0][indx+1]
                        elt["wind_speed"] = value[0][indx+2]
                    fill_emptiness(elt, "PRCP")
                    fill_emptiness(elt, "SNWD")
                    fill_emptiness(elt, "TAVG")
                    output_final.write(str(elt).replace("'", "\"") + ",")

    with open("output_final.json", 'rb+') as filehandle:
        filehandle.seek(-1, os.SEEK_END)
        filehandle.truncate()
        filehandle.write(b"]")


# parse_data("data.json")



