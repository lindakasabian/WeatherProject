import json
import psycopg2
import glob
import os
import datetime
from dct_station import dct_stations
from parser import *
from get_data import *


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
print(names)
pgdb = Database('postgres', 'postgres', 'leet1337', 'localhost')
connection, curs_1 = pgdb.connect()


def split_db(conn, cursor):
    for key, value in dct_stations.items():
        SQL = f"""CREATE TABLE {key} AS
      SELECT * FROM noaa_weather WHERE station = '{value}';"""
        cursor.execute(SQL)
    conn.commit()


def append_db(conn, cursor):
    with open("output_final.json", "r") as final_file:
        final_json = json.load(final_file)
        json_copy = final_json
        SQL = """INSERT INTO noaa_weather (date, tavg, station, prcp, snwd, wind_direction, wind_speed) 
                VALUES (%s, %s, %s, %s, %s, %s, %s);"""
    for items in json_copy:
        variables = (items["DATE"], items["TAVG"], items["STATION"], items["PRCP"],
                     items["SNWD"],
                     items["wind_direction"], items["wind_speed"])
        record = cursor.execute(SQL, variables)
    conn.commit()
    split_db(connection, curs_1)


# append_db(connection, curs_1)


def get_data(cities, start_date, end_date):
    """ if noaa has no data for period function won't behave normally """
    date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    global connection, curs_1
    if date < datetime.datetime(2020, 1, 20):
        for city in cities:
            SQL = f"""SELECT * FROM {city} WHERE station in (%s) and date BETWEEN %s AND %s;"""
            vars = (dct_stations[city], start_date, end_date)
            record = curs_1.execute(SQL, vars)
            rows = curs_1.fetchall()
            for row in rows:
                print(row)
            return rows
    else:
        if not os.path.exists(f"{cities[0]}.csv"):
            get_initial_data_wwo(names, '2020-01-21', '2020-01-22')
            for city, station in dct_stations.items():
                if os.path.exists(f"{city}.csv"):
                    os.rename(f"{city}.csv", f"wwo_data/{city}.csv")
        get_initial_data_noaa(URL_1, "&startDate=2020-01-21", '2020-01-22')
        if os.path.getsize('data.json') < 50:
            print("sorry, noaa didn't updated data yet")
           # parse_data_case_no_noaa()
        else:
            parse_data("data.json")
        # replace with date.today() after making


astana = get_data(["Astana"], '2020-01-21', '2020-01-22')

