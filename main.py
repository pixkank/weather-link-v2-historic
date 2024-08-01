from dotenv import load_dotenv
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

LOG_FILE_PATH = "./logs"
FORMAT_DATE_TIME = "%Y-%m-%d %H:%M"

logging.basicConfig(filename=f'{LOG_FILE_PATH}/error.log', level=logging.ERROR)


class GetApiWeatherLink:
    def get_now_dt(self):
        tz = pytz.timezone('Asia/Bangkok')
        dt_now_formated = datetime.now(tz).strftime(FORMAT_DATE_TIME)
        dt_now = datetime.strptime(dt_now_formated, FORMAT_DATE_TIME)
        diff_mod_5_min = dt_now.minute % 5
        if diff_mod_5_min == 0:
            diff_mod_5_min = 5
        dt = dt_now - timedelta(minutes=diff_mod_5_min)
        return dt

    def convert_datetime_to_ts(self, dt):
        return int(datetime.timestamp(dt))

    def get_start_end_timestamp(self, dt):
        start_timestamp = self.convert_datetime_to_ts(dt - timedelta(minutes=5))
        end_timestamp = self.convert_datetime_to_ts(dt)
        return start_timestamp, end_timestamp

    def get_weather_api_data(self, start_timestamp, end_timestamp):
        url = f"https://api.weatherlink.com/v2/historic/{os.getenv('station-id')}?api-key={os.getenv('api-key')}&start-timestamp={start_timestamp}&end-timestamp={end_timestamp}"
        payload = {}
        headers = {"X-Api-Secret": os.getenv('x-api-secret')}
        response = requests.request("GET", url, headers=headers, data=payload)
        return response.json()

    def get_weather_data(self, start_timestamp, end_timestamp):
        data_list = {}
        tranformed_data_list = {}
        weather_data = self.get_weather_api_data(start_timestamp, end_timestamp)
        df = pd.DataFrame(weather_data["sensors"], columns=["data"])
        for data in df["data"]:
            if len(data) > 0:
                if data[0].get("temp_last") is not None:
                    data_list["temp_last"] = data[0].get("temp_last")
                if data[0].get("hum_last") is not None:
                    data_list["hum_last"] = data[0].get("hum_last")
                if data[0].get("wind_speed_avg") is not None:
                    data_list["wind_speed_avg"] = data[0].get("wind_speed_avg")
                if data[0].get("wind_dir_of_prevail") is not None:
                    data_list["wind_dir_of_prevail"] = data[0].get("wind_dir_of_prevail")
                if data[0].get("heat_index_hi") is not None:
                    data_list["heat_index_hi"] = data[0].get("heat_index_hi")
                if data[0].get("rainfall_mm") is not None:
                    data_list["rainfall_mm"] = data[0].get("rainfall_mm")
                if data[0].get("rain_rate_hi_mm") is not None:
                    data_list["rain_rate_hi_mm"] = data[0].get("rain_rate_hi_mm")
                if data[0].get("bar_sea_level") is not None:
                    data_list["bar_sea_level"] = data[0].get("bar_sea_level")
        if len(data_list) > 0:
            tranformed_data_list = self.tranform_data_list(data_list)
            dt = datetime.fromtimestamp(end_timestamp)
            tranformed_data_list["dt"] = dt.strftime(FORMAT_DATE_TIME)
        return tranformed_data_list

    def convert_temp_f_to_c(self, temp):
        return (temp - 32) * (5 / 9)

    def convert_ws_mph_to_ms(self, mph):
        return mph * 0.44704

    def convert_wd_to_text(self, wd):
        direction = "None"
        if wd is not None:
            if wd >= 348.5 or wd <= 11.25:
                direction = "N"
            elif wd >= 11.25 and wd <= 33.75:
                direction = "NNE"
            elif wd >= 33.75 and wd <= 56.25:
                direction = "NE"
            elif wd >= 56.25 and wd <= 78.75:
                direction = "ENE"
            elif wd >= 78.75 and wd <= 101.25:
                direction = "E"
            elif wd >= 101.25 and wd <= 123.75:
                direction = "ESE"
            elif wd >= 123.75 and wd <= 146.25:
                direction = "SE"
            elif wd >= 146.25 and wd <= 168.75:
                direction = "SSE"
            elif wd >= 168.75 and wd <= 191.25:
                direction = "S"
            elif wd >= 191.25 and wd <= 213.75:
                direction = "SSW"
            elif wd >= 213.75 and wd <= 236.25:
                direction = "SW"
            elif wd >= 236.25 and wd <= 258.75:
                direction = "WSW"
            elif wd >= 258.75 and wd <= 281.25:
                direction = "W"
            elif wd >= 281.25 and wd <= 303.75:
                direction = "WNW"
            elif wd >= 303.75 and wd <= 326.25:
                direction = "NW"
            elif wd >= 326.25 and wd <= 348.5:
                direction = "NNW"
        return direction

    def convert_inHg_to_hPa(self, inHg):
        return inHg * 33.864

    def tranform_data_list(self, data_list):
        value_list = {}
        if len(data_list) > 0:
            temperature_last = self.convert_temp_f_to_c(data_list.get("temp_last"))
            value_list["temperature_last"] = np.round(temperature_last, 2)
            humidity = data_list.get("hum_last")
            value_list["humidity"] = humidity
            ws = self.convert_ws_mph_to_ms(data_list.get("wind_speed_avg"))
            value_list["wind_speed"] = np.round(ws, 2)
            wd = self.convert_wd_to_text(data_list.get("wind_dir_of_prevail"))
            value_list["wind_direction"] = wd
            heat_index_hi = self.convert_temp_f_to_c(data_list.get("heat_index_hi"))
            value_list["heat_index_hi"] = np.round(heat_index_hi, 2)
            rain = data_list.get("rainfall_mm")
            value_list["rain"] = rain
            rain_rate = data_list.get("rain_rate_hi_mm")
            value_list["rain_rate"] = rain_rate
            pressure = self.convert_inHg_to_hPa(data_list.get("bar_sea_level"))
            value_list["pressure"] = np.round(pressure, 2)
        return value_list

    def post_data(self,data_list):
        # some post to database or api
        with open(f"{LOG_FILE_PATH}/data_log.txt", "a") as fdata_log:
            fdata_log.write(str(data_list) + "\n")
    
    def no_data(self,end_timestamp):
        with open(f"{LOG_FILE_PATH}/nodata_log.txt", "a") as fnodata_log:
            fnodata_log.write(datetime.fromtimestamp(end_timestamp).strftime(FORMAT_DATE_TIME) + "\n")

    def get_data(self):
        try:
            dt = self.get_now_dt()
            start_timestamp, end_timestamp = self.get_start_end_timestamp(dt)
            data_list = self.get_weather_data(start_timestamp, end_timestamp)
            if len(data_list) > 0:
                self.post_data(data_list)
            else:
                self.no_data(end_timestamp)
            with open(f"{LOG_FILE_PATH}/program_run_log.txt", "a") as fprogram_run_log:
                fprogram_run_log.write(datetime.fromtimestamp(end_timestamp).strftime(FORMAT_DATE_TIME) + "\n")
                fprogram_run_log.close()
            self.check_lost_data()
        except Exception as e:
            logging.error(e)

    def check_no_data(self):
        if os.path.exists(f"{LOG_FILE_PATH}/nodata_log.txt"):
            with open(f"{LOG_FILE_PATH}/nodata_log.txt", "r+") as f:
                lines = f.readlines()
                f.seek(0)
                for line in lines:
                    if line.strip():
                        nodata_datetime = line.strip()
                        dt = datetime.strptime(nodata_datetime, FORMAT_DATE_TIME)
                        start_timestamp, end_timestamp = self.get_start_end_timestamp(dt)
                        data_list = self.get_weather_data(start_timestamp, end_timestamp)
                        if len(data_list) > 0:
                            self.post_data(data_list)
                        else:
                            f.write(line)
                    else:
                        f.write(line)
                f.truncate()

    def check_lost_data(self):
        if os.path.exists(f"{LOG_FILE_PATH}/program_run_log.txt"):
            with open(f"{LOG_FILE_PATH}/program_run_log.txt", "r+") as fprogram_run_log:
                lines = fprogram_run_log.readlines()[-2:]
                fprogram_run_log.seek(0)
                fprogram_run_log.writelines(lines)
                fprogram_run_log.truncate()
                fprogram_run_log.close()
            if len(lines) > 1:
                last_dt = datetime.strptime(lines[1].strip(), FORMAT_DATE_TIME)
                last_dt = last_dt - timedelta(minutes=5)
                old_dt = datetime.strptime(lines[0].strip(), FORMAT_DATE_TIME)
                if last_dt > old_dt:
                    diff = last_dt - old_dt
                    count = int((diff.seconds / 60) / 5)
                    for i in range(count):
                        start_timestamp, end_timestamp = self.get_start_end_timestamp(last_dt)
                        data_list = self.get_weather_data(start_timestamp, end_timestamp)
                        if len(data_list) > 0:
                            self.post_data(data_list)
                        else:
                            self.no_data(end_timestamp)
                        last_dt = last_dt - timedelta(minutes=5)

    def begin(self):
        while True:
            thread1 = threading.Thread(target=self.get_data)
            thread2 = threading.Thread(target=self.check_no_data)
            thread1.start()
            thread2.start()
            time.sleep(300)


if __name__ == "__main__":
    app = GetApiWeatherLink()
    app.begin()
    # Keep the main thread alive
    while True:
        time.sleep(1)
