import datetime
import glob

import numpy as np
import pandas as pd
import psycopg2

from get_data import *
from parser import *

LAST_DATE = "2020-01-20"


def convert_to_dict(pandas_data_frame, regime, flag=True):
    dict_to_convert_to = {}
    dct = pandas_data_frame.to_dict('index')
    for key, value in dct.items():
        readable_key = datetime.datetime.strptime(str(key), "%Y-%m-%d %H:%M:%S")
        if regime == "max":
            if flag:
                dict_to_convert_to[f"Средний максимум температуры за {readable_key.year} год"] = value
            else:
                dict_to_convert_to[readable_key] = value
        elif regime == "min":
            if flag:
                dict_to_convert_to[f"Средний минимум температуры за {readable_key.year} год"] = value
            else:
                dict_to_convert_to[readable_key] = value
        elif regime == "close":
            if flag:
                dict_to_convert_to[readable_key.year] = value
            else:
                dict_to_convert_to[readable_key] = value
    return dict_to_convert_to


class Database:
    def __init__(self, dbname, user, passwd, host):
        self.dbname = dbname
        self.user = user
        self.passwd = passwd
        self.host = host

    def connect(self):
        conn = psycopg2.connect(dbname=self.dbname, user=self.user,
                                password=self.passwd, host=self.host)
        cursor = conn.cursor()
        return conn, cursor


names = [os.path.splitext(x)[0][9:] for x in glob.glob('archived/*')]
pgdb = Database('weather', 'root', 'password', 'db')
connection, curs_1 = pgdb.connect()


def get_data(cities, start_date, end_date):
    cities = list(filter(None, cities))
    """ if noaa has no data for period function won't behave normally """
    last_date_of_observations = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    global connection, curs_1, LAST_DATE
    if last_date_of_observations < datetime.datetime(2020, 1, 21):
        stations = [dct_stations[city] for city in cities]
        if len(stations) > 1:
            cities = tuple(list(stations))
        elif len(stations) == 1:
            cities = f"(\'{stations[0]}\')"
        sql_statement = f"""SELECT * FROM noaa_weather WHERE station in {cities} and date BETWEEN '{start_date}' AND 
        '{end_date}';"""
        sql_statement_2 = f"""SELECT * FROM noaa_weather WHERE station in {cities} and date in ('{LAST_DATE}');"""
        statement_2 = curs_1.execute(sql_statement)
        rows_1 = curs_1.fetchall()
        statement_3 = curs_1.execute(sql_statement_2)
        rows_2 = curs_1.fetchall()
        return rows_1, rows_2
    else:
        if not os.path.exists(f"{cities[0]}.csv"):
            get_initial_data_wwo(names, '2020-01-21', '2020-01-22')
            for city, station in dct_stations.items():
                if os.path.exists(f"{city}.csv"):
                    os.rename(f"{city}.csv", f"wwo_data/{city}.csv")
        get_initial_data_noaa(URL_1, "&startDate=2020-01-21", '2020-01-22')
        if os.path.getsize('data.json') < 50:
            print("sorry, noaa didn't updated data yet")
        else:
            parse_data("data.json")


def get_weather_stats(input_tables, today_tables):
    def create_dataframes(table):
        df = pd.DataFrame(table)
        UniqueNames = df[2].unique()
        dictofdataframes = {elem: pd.DataFrame for elem in UniqueNames}
        for key in dictofdataframes.keys():
            dictofdataframes[key] = df[:][df[2] == key]
        return dictofdataframes

    DataFrameDict = create_dataframes(input_tables)
    TodayInputDict = create_dataframes(today_tables)
    weatherstats_dict_handler = []
    for key, value in DataFrameDict.items():
        storing_dct = {}
        storing_lst = []
        today_weather = TodayInputDict[key][1].iloc[0]
        df = value.copy()
        maximum = df[1].max()
        avg = df[1].sum() / len(df[1])
        minimum = df[1].min()
        date_diff = df[0].iloc[-1] - df[0].iloc[0]
        compare_df = df[[0, 1, 2, 3, 4, 5, 6, 7, 8]]

        def detect_weather(input_df, conditions, result_1, result_2):
            input_df = np.where(conditions, result_1, result_2)
            unique, counts = np.unique(input_df, return_counts=True)
            weather_detected = dict(zip(unique, counts))
            try:
                weather_num = weather_detected[result_1]
            except KeyError:
                weather_num = 0
            return weather_num

        # http://www.wxonline.info/ebook/AppendixB_final.pdf
        # NOAA stopped observing some cities' precipitation
        frzr_num = detect_weather(compare_df, ((compare_df[3] > 0.0) & (compare_df[1] > 0.0) & (compare_df[7] < 3.0) &
                                               (compare_df[8] > -2.0)), "Freezing rain", "None")
        rain_num = detect_weather(compare_df, ((compare_df[3] > 0.0) & (compare_df[1] > 0.0)), "Rain", "None")
        snow_num = detect_weather(compare_df, ((compare_df[3] > 0.0) & (compare_df[1] < 0.0)), "Snow", "None")
        pellets_num = detect_weather(compare_df,
                                     ((compare_df[3] > 0.0) & (compare_df[1] > 0.0) & (compare_df[7] < 1.0) &
                                      (compare_df[8] > -5.0)), "Ice pellets", "None")
        weather_dct = {"frzr": frzr_num, "rain": rain_num, "snow": snow_num, "pellets": pellets_num}
        most_freq_weather = sorted(weather_dct)[2:]

        avg_wind = df[6].sum() / len(df[6])
        wd_count = compare_df.groupby(5).size()
        wd_frequent = wd_count.idxmax()

        prec = compare_df[(compare_df[3] > 0.0)].count()[0]
        prec_count = float(prec / date_diff.days)
        prec_no = 1 - prec_count

        storing_lst.append([prec_count, prec_no])
        storing_lst.append(most_freq_weather)
        storing_lst.append([avg_wind, wd_frequent])
        storing_lst.append(maximum)
        storing_lst.append(avg)
        storing_lst.append(minimum)
        storing_lst.append(today_weather)

        df[0] = pd.to_datetime(df[0])
        df.set_index(df[0], inplace=True)

        df_sort = df.iloc[(df[1] - today_weather).abs().argsort()[:10]]
        closest_dct = convert_to_dict(df_sort[[1]].copy(), "close", False)
        storing_lst.append(closest_dct)

        if date_diff >= datetime.timedelta(730):
            subdf_max = df[[7]].copy()
            subdf_min = df[[8]].copy()
            mean_max = subdf_max.groupby(pd.Grouper(freq='Y')).mean()
            mean_min = subdf_min.groupby(pd.Grouper(freq='Y')).mean()
            new_mmax = convert_to_dict(mean_max, "max")
            new_mmin = convert_to_dict(mean_min, "min")
            storing_lst.append(new_mmax)
            storing_lst.append(new_mmin)
        storing_dct[key] = storing_lst
        weatherstats_dict_handler.append(storing_dct)
    return weatherstats_dict_handler


def handle_results(lst):
    lst_key_handler, lst_handler, dct_handler = [], [], []
    lst_value_handler = [v for x in lst for k, v in x.items()]
    lst_temp_key_handler = [k for x in lst for k, v in x.items()]
    for key, value in dct_stations.items():
        for items in lst_temp_key_handler:
            if items == value:
                lst_key_handler.append(key)
    for items in lst_value_handler:
        lst_out = []
        weather_close, dct_out, weather_max, weather_min = {}, {}, {}, {}
        perc_percent = items[0][0] * 100
        clear_perc = items[0][1] * 100
        freq_wea = items[1]
        avg_wind = items[2][0]
        avg_dir = items[2][1]
        temp_max = items[3]
        temp_avg = items[4]
        temp_min = items[5]
        weather_today = items[6]
        dct_out.update({"процент осадков": round(perc_percent, 2), "процент безоблачных дней": round(clear_perc, 2),
                        "частая погода": freq_wea,
                        "средняя скорость ветра": round(avg_wind, 2), "частое направление ветра": avg_dir,
                        "максимум температуры": round(temp_max, 2),
                        "среднее температуры": round(temp_avg, 2), "минимум температуры": round(temp_min, 2),
                        "дни с близкой погодой": weather_today})
        lst_out.append(dct_out)
        for k, v in items[7].items():
            weather_close[k.date()] = v[1]

        def handle_weather(input_weather, storage):
            for k, v in input_weather.items():
                storage[k] = [round(v, 2) for k, v in v.items()][0]
            return storage
        lst_out.append(weather_close)
        if len(items) == 10:
            lst_out.append(handle_weather(items[8], weather_max))
            lst_out.append(handle_weather(items[9], weather_min))
            lst_handler.append(lst_out)
        else:
            lst_handler.append(lst_out)
        del weather_close, dct_out, weather_max, weather_min, lst_out
    dictionary = dict(zip(lst_key_handler, lst_handler))
    return dictionary


def process(city, start, end):
    rows, today = get_data(city, start, end)
    lst = get_weather_stats(rows, today)
    dct = handle_results(lst)
    return dct
