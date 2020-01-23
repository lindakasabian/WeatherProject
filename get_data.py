import requests
import os
from datetime import date
from wwo_hist import retrieve_hist_data
from urllib.error import HTTPError
from dct_station import dct_stations

URL_1 = "https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries"
LOC_LIST = ["Volgograd"]
START = '20-01-2020'
END = date.today()


def get_initial_data_noaa(url, start_date, end_date):
    stations = "&stations="
    location_list = []
    for key, value in dct_stations.items():
        stations += f"{value},"
        location_list.append(key)
    stations = stations[:-1]
    startDate = start_date
    endDate = f"&endDate={end_date}"
    type_json = "&format=json"
    units = "&units=metric"
    complete_request = f"{URL_1}{stations}{startDate}{endDate}{type_json}&includeStationName=true{units}"
    response_1 = requests.get(complete_request)
    raw_json = response_1.json()
    with open("data.json", "w") as saved_json:
        saved_json.write("[")
        for elt in raw_json:
            if elt["NAME"] == "KALININGRAD, RS":
                pass
            saved_json.write(str(elt).replace("'", "\"") + ",")

    with open("data.json", 'rb+') as filehandle:
        filehandle.seek(-1, os.SEEK_END)
        filehandle.truncate()
        filehandle.write(b"]")


def get_initial_data_wwo(locations, start_date, end_date):
    frequency = 24
    api_key_1 = '1d5cc937de61480aaf903645202201'
    api_key_2 = '340dfd23ecc94a4a8c631942202301'
    api_key_3 = 'e9c547b9ae304c07b9834623202301'
    try:
        api_key = api_key_3
        hist_weather_data = retrieve_hist_data(api_key,
                                               locations,
                                               start_date,
                                               end_date,
                                               frequency,
                                               location_label=False,
                                               export_csv=True,
                                               store_df=True)
    except HTTPError:
        print('bad api key, try another one')


# get_initial_data_wwo(LOC_LIST, START, END)

# get_initial_data_noaa(URL_1, "&startDate=2010-01-01", '2020-01-20')

