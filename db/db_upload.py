import json

import psycopg2


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


pgdb = Database('postgres', 'postgres', 'password', 'db')
connection, curs_1 = pgdb.connect()


def append_db(conn, cursor):
    with open("output_final.json", "r") as final_file:
        final_json = json.load(final_file)
        json_copy = final_json
        sql_statement = """INSERT INTO noaa_weather (date, tavg, station, prcp, snwd, wind_direction, wind_speed, tmax, tmin) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    for json_rows in json_copy:
        variables = (json_rows["DATE"], json_rows["TAVG"], json_rows["STATION"], json_rows["PRCP"],
                     json_rows["SNWD"],
                     json_rows["wind_direction"], json_rows["wind_speed"], json_rows["TMAX"], json_rows["TMIN"])
        statement_1 = cursor.execute(sql_statement, variables)
    conn.commit()


append_db(connection, curs_1)
