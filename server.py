import time
import requests
from database import DataBase
from parser_WB import ParseWB
from flask import request

db = DataBase()
API_ADDRESS_APP = 'http://127.0.0.1:4000/api/v1/'

if __name__ == '__main__':

    while True:
        time.sleep(60)

        result = db.get_value_temp_parser()

        if result is not None:

            user_id = result[0]
            url_wb = result[1]
            date_req = result[2]
            regions = result[3]
            gender = result[4]
            age = result[5]
            budget = result[6]

            ParseWB(url_wb).parse()

            datajson = {
                'user_id': user_id,
                'url_wb': url_wb,
                'regions': regions,
                'gender': gender,
                'age': age,
                'budget': budget
                }

            method = 'ads_info/update'
            print('Отправил запрос на обновление')
            status_update = requests.post(API_ADDRESS_APP + method, json=datajson)

            if status_update.status_code == 200:
                is_delete = db.delete_value_temp_parser(user_id=user_id, url_wb=url_wb, date_req=date_req)
