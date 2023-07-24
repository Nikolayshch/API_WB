from flask import Flask, jsonify, request
from parser_WB import ParseWB
from datetime import datetime
from database import DataBase

app = Flask(__name__)
db = DataBase()

def print_hi(name):
    #token = 'y0_AgAAAAAbokf1AAoGEgAAAADlIN3ZTCghoZDpSkehhSZQgmWTiNBEop8'
    token = 'y0_AgAAAABvRb1KAAo1dgAAAADoOuLcg5eg4mk0RCOFicz7xyRSqFLEX20' # Боевой токен AISELLER.RU

    # Логин клиента рекламного агентства
    # Обязательный параметр, если запросы выполняются от имени рекламного агентства
    #clientLogin = 'olga.1992.popova'
    clientLogin = 'aisellerru'
    # Use a breakpoint in the code line below to debug your script.
    #ya = api.API_Yandex_back(token=token, clientLogin=clientLogin)
    #print(ya)
    #camp = ya.get_campaigns()
    Client_name = str('client_id_123')
    Name_Group_ads = Client_name + '_GroupsADS'
    url_market = 'https://www.wildberries.ru/seller/46346'

    #id_company = add_campaigns['AddResults'][0]['Id']
    #print('------------Создана кампания {0}------------------'.format(id_company))

    #result_ads_groups = ya.add_ads_groups(Name_Group=Name_Group_ads, CampaignId=id_company, url_market=url_market)

    #print(result_ads_groups)
# Press the green button in the gutter to run the script.

@app.route('/api/v1/parser_wb_start', methods=['POST'])
def parser_wb_start():
    print(request.json)


    if request.json and 'user_id' in request.json and 'url_wb' in request.json:
        jsonData = request.json
        is_add_temp = db.add_value_temp_parser(user_id=jsonData['user_id'], url_wb=jsonData['url_wb'],
                                        regions=jsonData['regions'], gender=jsonData['gender'], age=jsonData['age'],
                                        budget=jsonData['budget'])
        print('Записал temp')
        if is_add_temp:
            return jsonify({'status_code': 201})

    return jsonify({'status_code': 503})

if __name__ == '__main__':
    db.creat_table_temp()
    app.run(port=3000, debug=True)
