import requests
import pyodbc
from datetime import datetime
import configparser
import pytz
import time



# loading configuration file
config = configparser.ConfigParser()
config.read('config.ini')
base_url = config["DEFAULT"]["HOMEMANAGER_BASE_URL"]
dataset_base_url = config["DEFAULT"]["DATASET_BASE_URL"]

# you can edit user and token information.
user = config["DEFAULT"]["user"]
token = config["DEFAULT"]["TOKEN"]


dataset_url = dataset_base_url + 'user=' + user + '&token=' + token
# sending get request and saving the response as response object
datasets = requests.get(url=dataset_url)
# extracting data in json format
datasets_response = datasets.json()
sourceID = datasets_response["data"][0]["datasets"][0]["sourceID"]
endpoint = datasets_response["data"][0]["datasets"][0]["endpoint"]
SourceType = datasets_response["data"][0]["datasets"][0]["sourceType"]
version = datasets_response["data"][0]["datasets"][0]["version"]
house_id = datasets_response["data"][0]["house_id"]
gateway = datasets_response["data"][0]["gateway"]


db_connect_inf = 'Driver={SQL Server};' 'Server=ewiienergi.database.windows.net;' 'Database=EWIIEjendomme;' 'uid=EWIIEnergi;' 'pwd=Kokbjerg30;'
conn = pyodbc.connect(db_connect_inf)
cursor = conn.cursor()

def home_manager_15min():
    url = base_url + 'user=' + user + '&token=' + token + '&sourceID=' + sourceID + '&endpoint=' + endpoint
    # sending get request and saving the response as response object
    r = requests.get(url=url)
    # extracting data in json format
    response = r.json()
    data = (response["data"][0])
    SQLCommand = (
        "INSERT INTO NorthQ_Datapoints_15min (DatasetID, Endpoint, SourceType, SourceID, version, getway, Time_stamp, HomeValue, Date_time, HourDK) VALUES (?,?,?,?,?,?,?,?,?,?);")
    DatasetID = sourceID + endpoint
    Timestamp = data["timestamp"]
    Homevalue = data["value"]
    Datetime = datetime.fromtimestamp(data["timestamp"], pytz.utc)
    HourDK = datetime.fromtimestamp((int(data["timestamp"]) - 3600), pytz.utc)

    Values = [
        str(DatasetID).replace('.', ','),
        endpoint,
        SourceType,
        sourceID,
        version,
        gateway,
        Timestamp,
        str(Homevalue).replace('.', ','),
        Datetime,
        HourDK
    ]
    print(Values)
    cursor.execute(SQLCommand, Values)
    conn.commit()

if __name__ == '__main__':
    while (True):
        try:
            home_manager_15min()
            # you can set times. 15min = 900
            time.sleep(900)
        except:
            print("Proxy Error appeared. Try again.")
            continue


