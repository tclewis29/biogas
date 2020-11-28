import os
import time
import glob
import time
import mysql.connector as mariadb
import sys
from collections import OrderedDict

os.system('modprobe w1-gpio')
os.system('modprobe w1-therm')

# List sensors in the system:
sensors = OrderedDict([("temp_1", {  # DS18B20 Temperature Sensor
                            "sensor_type": "1_wire_temp",
                            "name": "ambient_temp",
                            "is_connected": True,
                            "is_ref": False,
                            "ambient_temp":
                            "sys/bus/w1/devices/28-031501c743ff/w1_slave",
                            "accuracy": 1}),

                            ("temp_2", {  # DS18B20 Temperature Sensor
                            "sensor_type": "1_wire_temp",
                            "name": "digester_temp",
                            "is_connected": True,
                            "is_ref": False,
                            "digester_temp":
                            "sys/bus/w1/devices/28-041501cae2ff/wl_slave",
                            "accuracy": 1})])

def create_database():

    conn = mariadb.connect(user=username,
                           password=password,
                           host=servername)
    curs = conn.cursor()
    try:
        curs.execute("SET sql_notes = 0; ")  # Hide Warnings
        curs.execute("CREATE DATABASE IF NOT EXISTS {}".format(dbname))
        curs.execute("SET sql_notes = 1; ")  # Show Warnings
    except mariadb.Error as error:
        print("Error: {}".format(error))
        pass
    conn.commit()
    conn.close()
    return

def open_database_connection():
# Variables for MySQL
    connection = mariadb.connect(host=servername, user=username,password=password, database=dbname)
    cursor = connection.cursor()
    try:
        cursor.execute("SET sql_notes = 0; ")  # Hide Warnings
    except mariadb.Error as error:
        print("Error: {}".format(error))
        pass

    return connection, cursor

def create_sensors_table():

    connection, cursor = open_database_connection()
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS sensors (timestamp DATETIME);")
    except mariadb.Error as error:
        print("Error: {}".format(error))
        pass

    for key, value in list(sensors.items()):
        if value["is_connected"] is True:
            try:
                cursor.execute("ALTER TABLE sensors ADD {} DECIMAL(10,2);"
                .format(value["name"]))
            except mariadb.Error as error:
                print("Error: {}".format(error))
                pass

    close_database_connection(connection, cursor)

    return

def close_database_connection():

    try:
        cursor.execute("SET sql_notes = 1; ")  # Show Warnings
    except mariadb.Error as error:
        print("Error: {}".format(error))
        pass
    connection.commit()
    connection.close()

def read_temp_raw(temp_num):
    f = open(sensors[temp_num]["name"], 'r')
    lines = f.readlines()
    f.close()
    return lines

def read_temp(temp_num):
    lines = read_temp_raw(temp_num)
    while lines[0].strip()[-3:] != 'YES':
        time.sleep(5)
        lines = read_temp_raw(temp_num)
    equals_pos = lines[1].find('t=')
    if equals_pos != -1:
        temp_string = lines[1][equals_pos+2:]
        temp_c = float(temp_string) / 1000.0
        return temp_c

def log_sensor_readings(all_curr_readings):

    # Create a timestamp and store all readings on the MySQL database

    connection, cursor = open_database_connection()
    try:
        cursor.execute("INSERT INTO sensors (timestamp) VALUES(now());")
        cursor.execute("SELECT MAX(timestamp) FROM sensors")
    except mariadb.Error as error:
        print("Error: {}".format(error))
        pass
    last_timestamp = cursor.fetchone()
    last_timestamp = last_timestamp[0].strftime('%Y-%m-%d %H:%M:%S')

    for readings in all_curr_readings:
        try:
            cursor.execute(("UPDATE sensors SET {} = {} WHERE timestamp = '{}'")
                        .format(readings[0], readings[1], last_timestamp))
        except mariadb.Error as error:
            print("Error: {}".format(error))
            pass

    close_database_connection(connection, cursor)

    return

def read_sensors():

    all_curr_readings = []

    # Get the readings from any 1-Wire temperature sensors

    for key, value in sensors.items():
        if value["is_connected"] is True:
            if value["sensor_type"] == "1_wire_temp":
                try:
                    sensor_reading = (round(float(read_temp(key)),
                                 value["accuracy"]))
                except:
                    sensor_reading = 50
                    
                all_curr_readings.append([value["name"], sensor_reading])
    
    log_sensor_readings(all_curr_readings)

    return

# Define MySQL database login settings

servername = "localhost"
username = "getmeoffgrid"
password = "/home/pi/Documents/sample.txt"
dbname = "sensors"

while True:  # Repeat the code indefinitely

    if loops == 300:
        loops = 0

        read_sensors()

    loops += 1
    time.sleep(5)