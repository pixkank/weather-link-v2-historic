from dotenv import load_dotenv
import json
import logging
import requests
import pandas as pd
import numpy as np
import threading
import time
import pytz
import os
from datetime import datetime, timedelta

load_dotenv()

class GetApiWeatherLink:
    def __init__(self) -> None:
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.LOG_FILE_PATH = f"{ROOT_DIR}/logs"
        self.FORMAT_DATE_TIME = "%Y-%m-%d %H:%M"
        self.FORMAT_DATE = "%Y-%m-%d"
        self.FORMAT_TIME = "%H:%M"
        logging.basicConfig(filename=f"{self.LOG_FILE_PATH}/error.log",
                            level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")
        # set parameter & api
        self.met_parameter = ["temp_last", "hum_last", "wind_speed_avg", "wind_dir_of_prevail",
                              "heat_index_hi", "rainfall_mm", "rain_rate_hi_mm", "bar_sea_level",
                              "uv_index_avg","solar_rad_avg"]
        self.api_weather_link = []
        api_1 = {"STATION_ID": os.getenv('STATION_ID_1'), "API_KEY": os.getenv('API_KEY_1'),
                 "X_API_SECRET": os.getenv('X_API_SECRET_1'), "DEVICE_ID": os.getenv('DEVICE_ID_1')}
        api_2 = {"STATION_ID": os.getenv('STATION_ID_2'), "API_KEY": os.getenv('API_KEY_2'),
                 "X_API_SECRET": os.getenv('X_API_SECRET_2'), "DEVICE_ID": os.getenv('DEVICE_ID_2')}
        self.api_weather_link.append(api_1)
        self.api_weather_link.append(api_2)
        pass

    def get_now_dt(self):
        tz = pytz.timezone("Asia/Bangkok")
        dt_now_formated = datetime.now(tz).strftime(self.FORMAT_DATE_TIME)
        dt_now = datetime.strptime(dt_now_formated, self.FORMAT_DATE_TIME)
        diff_mod_5_min = dt_now.minute % 5
        if diff_mod_5_min == 0:
            diff_mod_5_min = 5
            pass
        dt = dt_now - timedelta(minutes=diff_mod_5_min)
        return dt
    
    def convert_datetime_to_ts(self, dt):
        return int(datetime.timestamp(dt))
    
    def convert_ts_to_datetime(self,ts):
        dt = datetime.fromtimestamp(ts)
        dt = dt + timedelta(hours=7)
        return dt
    
    def get_start_end_timestamp(self, dt):
        # If Thai Time Zone  UTC 7 -> Sensor UTC 0
        dt = dt - timedelta(hours=7)
        start_timestamp = self.convert_datetime_to_ts(dt-timedelta(minutes=5))
        end_timestamp = self.convert_datetime_to_ts(dt)
        return start_timestamp, end_timestamp

    def convert_temp_f_to_c(self, temp):
        return (temp - 32) * (5 / 9)

    def convert_ws_mph_to_ms(self, mph):
        return mph * 0.44704

    def convert_inHg_to_hPa(self, inHg):
        return inHg * 33.864

    def tranform_data_list(self, data_list):
        value_list = {}
        if len(data_list) > 0:
            if data_list.get("temp_last") is not None:
                temperature_last = self.convert_temp_f_to_c(data_list.get("temp_last"))
                value_list["temperature"] = np.round(temperature_last, 2)
                pass
            if data_list.get("hum_last") is not None:
                humidity = data_list.get("hum_last")
                value_list["humidity"] = humidity
                pass
            if data_list.get("wind_speed_avg") is not None:
                wind_speed = self.convert_ws_mph_to_ms(data_list.get("wind_speed_avg"))
                value_list["wind_speed"] = np.round(wind_speed, 2)
                pass
            if data_list.get("wind_dir_of_prevail") is not None:
                wind_direction = data_list.get("wind_dir_of_prevail")
                value_list["wind_direction"] = wind_direction
                pass
            if data_list.get("heat_index_hi") is not None:
                heat_index_hi = self.convert_temp_f_to_c(data_list.get("heat_index_hi"))
                value_list["heat_index"] = np.round(heat_index_hi, 2)
                pass
            if data_list.get("rainfall_mm") is not None:
                rain = data_list.get("rainfall_mm")
                value_list["rain"] = rain
                pass
            if data_list.get("rain_rate_hi_mm") is not None:
                rain_rate = data_list.get("rain_rate_hi_mm")
                value_list["rain_rate"] = rain_rate
                pass
            if data_list.get("bar_sea_level") is not None:
                pressure = self.convert_inHg_to_hPa(data_list.get("bar_sea_level"))
                value_list["pressure"] = np.round(pressure, 2)
                pass
            if data_list.get("uv_index_avg") is not None:
                uv = data_list.get("uv_index_avg")
                value_list["uv"] = uv
                pass
            if data_list.get("solar_rad_avg") is not None:
                solar = data_list.get("solar_rad_avg")
                value_list["solar"] = solar
                pass
            pass
        return value_list    

    def get_weather_api_data(self, start_timestamp, end_timestamp, api):
        STATION_ID = api["STATION_ID"]
        API_KEY = api["API_KEY"]
        X_API_SECRET = api["X_API_SECRET"]
        
        url = f"https://api.weatherlink.com/v2/historic/{STATION_ID}?api-key={API_KEY}&start-timestamp={start_timestamp}&end-timestamp={end_timestamp}"
        payload = {}
        headers = {"X-Api-Secret": X_API_SECRET}
        response = requests.request("GET", url, headers=headers, data=payload)
        return response.json()

    def get_weather_data(self, start_timestamp, end_timestamp, api):
        data_list = {}
        tranformed_data_list = {}
        weather_data = self.get_weather_api_data(start_timestamp, end_timestamp, api)
        df = pd.DataFrame(weather_data["sensors"], columns=["data"])
        for data in df["data"]:
            if len(data) > 0:
                for parameter in self.met_parameter:
                    if data[0].get(parameter) is not None:
                        data_list[parameter] = data[0].get(parameter)
                        pass
                pass
            pass
        if len(data_list) > 0:
            tranformed_data_list = self.tranform_data_list(data_list)
            dt = datetime.fromtimestamp(end_timestamp)
            tranformed_data_list["data_date"] = dt.strftime(self.FORMAT_DATE)
            tranformed_data_list["data_time"] = dt.strftime(self.FORMAT_TIME)
            pass
        return tranformed_data_list
    
    def post_data(self, data_list, end_timestamp):
        api_post = os.getenv('URL_POST')

        headers = {
            "Content-Type": "application/json"
        }

        json_data = json.dumps(data_list)
        response = requests.post(api_post, headers=headers, data=json_data)

        if response.status_code != 200:
            self.no_data(end_timestamp,data_list["device_id"])
            device_id = data_list["device_id"]
            logging.error(f"DEVICE_ID : {device_id} | Error posting data: status code {response.status_code} | {response.text}")
            pass

        # log data
        with open(f"{self.LOG_FILE_PATH}/data_log.txt", "a") as fdata_log:
            fdata_log.write(str(data_list) + "\n")
            pass
        pass

    def no_data(self, end_timestamp, device_id):
        end_datetime = datetime.fromtimestamp(
            end_timestamp).strftime(self.FORMAT_DATE_TIME)
        with open(f"{self.LOG_FILE_PATH}/nodata_log.txt", "a") as fnodata_log:
            fnodata_log.write(device_id + " " + end_datetime + "\n")
            pass
        pass
    
    def get_data(self):
        try:
            dt = self.get_now_dt()
            start_timestamp, end_timestamp = self.get_start_end_timestamp(dt)
            for api in self.api_weather_link:
                data_list = self.get_weather_data(start_timestamp, end_timestamp, api)
                if len(data_list) > 0:
                    data_list["device_id"] = api["DEVICE_ID"]
                    self.post_data(data_list, end_timestamp)
                    pass
                else:
                    self.no_data(end_timestamp, api["DEVICE_ID"])
                    pass
                pass
            with open(f"{self.LOG_FILE_PATH}/program_run_log.txt", "a") as fprogram_run_log:
                fprogram_run_log.write(datetime.fromtimestamp(end_timestamp).strftime(self.FORMAT_DATE_TIME) + "\n")
                fprogram_run_log.close()
                pass
            self.check_lost_data()
            pass
        except Exception as e:
            logging.error(e)
            pass
        pass

    def check_lost_data(self):
        if os.path.exists(f"{self.LOG_FILE_PATH}/program_run_log.txt"):
            with open(f"{self.LOG_FILE_PATH}/program_run_log.txt", "r+") as fprogram_run_log:
                lines = fprogram_run_log.readlines()[-2:]
                fprogram_run_log.seek(0)
                fprogram_run_log.writelines(lines)
                fprogram_run_log.truncate()
                fprogram_run_log.close()
                pass
            if len(lines) > 1:
                last_dt = datetime.strptime(lines[1].strip(), self.FORMAT_DATE_TIME)
                last_dt = last_dt - timedelta(minutes=5)
                old_dt = datetime.strptime(lines[0].strip(), self.FORMAT_DATE_TIME)
                while last_dt > old_dt:
                    start_timestamp, end_timestamp = self.get_start_end_timestamp(last_dt)
                    for api in self.api_weather_link:
                        data_list = self.get_weather_data(start_timestamp, end_timestamp, api)
                        if len(data_list) > 0:
                            data_list["device_id"] = api["DEVICE_ID"]
                            self.post_data(data_list, end_timestamp)
                            pass
                        else:
                            self.no_data(end_timestamp, api["DEVICE_ID"])
                            pass
                        last_dt = last_dt - timedelta(minutes=5)
                        pass
                    pass
                pass
            pass
        pass

    def begin(self):
        while True:
            thread1 = threading.Thread(target=self.get_data)
            thread1.start()
            time.sleep(300)
            pass

if __name__ == "__main__":
    app = GetApiWeatherLink()
    try:
        print("weather-link-v2-historic-demo is working.")
        print("Press ctrl+c to stop the process.")
        app.begin()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.error("KeyboardInterrupt")
        pass
    except Exception as e:
        logging.error(e)
        pass