DROP TABLE IF EXISTS noaa_weather;
CREATE TABLE noaa_weather
(
    date date NOT NULL,
    tavg real,
    station character(11) NOT NULL,
    prcp real,
    snwd real,
    wind_direction character(2),
    wind_speed real,
    tmax real,
    tmin real
);
