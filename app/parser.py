import csv
import json
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
        wwo_list = []
        wwo_dict = []
        for row in readedcsv:
            wind_dir = row[-2]
            wind_speed = row[-1]
            wind_dir = degree_to_direction(wind_dir)
            date_wwo = row[0]
            wwo_dict.append(date_wwo)
            wwo_dict.append(wind_dir)
            wwo_dict.append(wind_speed)
            wwo_dict.append(file)
            wwo_list.append(wwo_dict)
        return wwo_list


def wwo_read_case_no_noaa(file):
    with open(f"wwo_data/{file}.csv") as csvfile:
        readedcsv = csv.reader(csvfile, delimiter=',')
        wwolist_no_noaa = []
        wwodict_no_noaa = []
        for row in readedcsv:
            wind_dir = row[-2]
            wind_speed = row[-1]
            wind_dir = degree_to_direction(wind_dir)
            date_wwo = row[0]
            max_t = row[1]
            min_t = row[2]
            try:
                avg_t = (int(max_t) + int(min_t)) / 2
            except ValueError:
                avg_t = '0.0'
            prec = row[-6]
            wwodict_no_noaa.append(date_wwo)
            wwodict_no_noaa.append(wind_dir)
            wwodict_no_noaa.append(wind_speed)
            wwodict_no_noaa.append(avg_t)
            wwodict_no_noaa.append(prec)
            wwolist_no_noaa.append(wwodict_no_noaa)
        return wwolist_no_noaa


def parse_data_case_no_noaa():
    dict_parse_data_no_noaa = {}
    list_parse_data_no_noaa = []

    for city, station in dct_stations.items():
        lst = wwo_read_case_no_noaa(city)[0][5:]
        list_parse_data_no_noaa.append(lst)

    with open("output_final.json", "w") as output_final_no_noaa_w:
        output_final_no_noaa_w.write("[")
        list_parse_data_no_noaa = list_parse_data_no_noaa
        for i, items in enumerate(list_parse_data_no_noaa):
            if i % 5 == 0:
                dict_parse_data_no_noaa["DATE"] = items[0]
            if i % 5 == 1:
                dict_parse_data_no_noaa["wind_direction"] = items[1]
            if i % 5 == 2:
                dict_parse_data_no_noaa["wind_speed"] = items[2]
            if i % 5 == 3:
                dict_parse_data_no_noaa["TAVG"] = items[3]
            if i % 5 == 4:
                dict_parse_data_no_noaa["PRCP"] = items[4]
            value = dict_parse_data_no_noaa
            output_final_no_noaa_w.write(str(value).replace("'", "\"") + ",")

    with open("output_final.json", 'rb+') as output_final_no_noaa_rb:
        output_final_no_noaa_rb.seek(-1, os.SEEK_END)
        output_final_no_noaa_rb.truncate()
        output_final_no_noaa_rb.write(b"]")

    with open("output_final.json", "r") as file:
        lst_resulted = json.load(file)
        lst_resulted = lst_resulted[4::4]
        print(lst_resulted)


def parse_data(data):
    with open(data, "r") as noaa_file:
        noaa_raw = json.load(noaa_file)
        noaa_json = noaa_raw

    dict_parse_data = {}
    for city, station in dct_stations.items():
        dict_parse_data[station] = wwo_read(city)

    with open("output_final.json", "w") as output_final:
        output_final.write("[")
        for key, value in dict_parse_data.items():
            for json_row in noaa_json:
                if json_row["STATION"] == key:
                    indx = value[0].index(json_row["DATE"])
                    if value[0][indx] == json_row["DATE"]:
                        json_row["wind_direction"] = value[0][indx + 1]
                        json_row["wind_speed"] = value[0][indx + 2]
                    fill_emptiness(json_row, "PRCP")
                    fill_emptiness(json_row, "SNWD")
                    fill_emptiness(json_row, "TAVG")
                    fill_emptiness(json_row, "TMAX")
                    fill_emptiness(json_row, "TMIN")
                    output_final.write(str(json_row).replace("'", "\"") + ",")

    with open("output_final.json", 'rb+') as filehandle:
        filehandle.seek(-1, os.SEEK_END)
        filehandle.truncate()
        filehandle.write(b"]")

# parse_data("data.json")
