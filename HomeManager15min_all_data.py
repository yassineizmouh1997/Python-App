import requests
import pyodbc
from datetime import datetime
import configparser
import pytz



# loading configuration file
config = configparser.ConfigParser()
config.read('config.ini')
base_url = config["DEFAULT"]["HOMEMANAGER_BASE_URL"]
dataset_base_url = config["DEFAULT"]["DATASET_BASE_URL"]
user = config["DEFAULT"]["user"]
token = config["DEFAULT"]["TOKEN"]


db_connect_inf = 'Driver={SQL Server};' 'Server=ewiienergi.database.windows.net;' 'Database=EWIIEjendomme;' 'uid=EWIIEnergi;' 'pwd=Kokbjerg30;'
conn = pyodbc.connect(db_connect_inf)
cursor = conn.cursor()

def home_manager_15min():
    dataset_url = dataset_base_url + 'user=' + user + '&token=' + token
    # sending get request and saving the response as response object
    datasets = requests.get(url=dataset_url)
    # extracting data in json format
    datasets_response = datasets.json()
    house_id = datasets_response["data"][0]["house_id"]
    gateway = datasets_response["data"][0]["gateway"]
    for datasest in datasets_response["data"][0]["datasets"]:
        url = base_url + 'user=' + user + '&token=' + token + '&sourceID=' + datasest["sourceID"] + '&endpoint=' + datasest["endpoint"]
        #sending get request and saving the response as response object
        r = requests.get(url=url)
        # extracting data in json format
        response = r.json()
        print(response["data"])
        for data in response["data"]:
            SQLCommand = ("INSERT INTO NorthQ_Datapoints_15min (DatasetID, Endpoint, SourceType, SourceID, version, getway, Time_stamp, HomeValue, Date_time, HourDk) VALUES (?,?,?,?,?,?,?,?,?,?);")
            DatasetID = datasest["sourceID"] + datasest["endpoint"]
            SourceType = datasest["sourceType"]
            version = datasest["version"]
            Timestamp = data["timestamp"]
            Homevalue = data["value"]
            Datetime = datetime.fromtimestamp(data["timestamp"], pytz.utc)
            HourDK = datetime.fromtimestamp((int(data["timestamp"]) - 3600), pytz.utc)

            Values = [
                str(DatasetID).replace('.', ','),
                datasest["endpoint"],
                SourceType,
                datasest["sourceID"],
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
    while(True):
        try:
            home_manager_15min()
            print("HomeManager all data successfully saved.")
            break
        except:
            print("Proxy Error appeared. Try again.")
            continue


