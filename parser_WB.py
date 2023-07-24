# -*- coding: utf-8 -*-
import os
from datetime import datetime
import requests
import re
import csv
from models_WB import Items
import pandas as pd
import database as db
import sqlite3

class ParseWB:

    def __init__(self, url: str, path_files='files/', database_name='aiseller.db'):
        self.seller_id = self.__get_seller_id(url)
        self.database = db.DataBase(db_name=database_name)
        self.path_files = path_files
        self.list_backet = ['basket-03', 'basket-10', 'basket-02', 'basket-01', 'basket-04', 'basket-05', 'basket-06',
                            'basket-07', 'basket-08', 'basket-09', 'basket-11', 'basket-12']

    @staticmethod
    def __get_item_id(url: str):
        regex = "(?<=catalog/).+(?=/detail)"
        item_id = re.search(regex, url)[0]
        return item_id

    @staticmethod
    def __get_path(path):

        path = str(path)

        if not os.path.exists(path):
            os.mkdir(path)

        return path

    @staticmethod
    def __get_hashtages(id_product: int):

        response = requests.get('https://search-tags.wildberries.ru/tags?nm_id={0}'.format(str(id_product)))
        if response.status_code == 200:
            return response.json()

        return []

    def get_card_product(self, id_product: int, id_saller: int, date_load: str):

        id_product_str = str(id_product)
        path_saller = self.__get_path(id_saller)
        # https://basket-10.wb.ru/vol1336/part133655/133655304/info/ru/card.json
        category_product = ''
        description = ''

        for backet in self.list_backet:

            if len(id_product_str) > 8:
                len_art = 6
                len_min_art = 4
            else:
                len_art = 5
                len_min_art = 3

            url = 'https://{0}.wb.ru/vol{1}/part{2}/{3}/info/ru/card.json'.format(backet,
                                                                                  id_product_str[:len_min_art],
                                                                                  id_product_str[:len_art],
                                                                                  id_product_str
                                                                                  )
            response = requests.get(url)

            if response.status_code == 200:
                json_card = response.json()

                if 'subj_root_name' in json_card:
                    category_product = json_card['subj_root_name']
                    if 'description' in json_card:
                        description = json_card['description']

                    self.database.add_value_card(date_load=date_load, id_seller=id_saller, id_product=id_product,
                                                 category_product=category_product, description=description,
                                                 json_file=str(json_card))
                    break

        return category_product, description

    def get_images(self, id_product: int, id_saller: int):

        global len_min_art
        id_product_str = str(id_product)
        path_saller = self.__get_path(id_saller)
        list_format_images = ['big', 'c516x688', 'c246x328']
        status_code = 404
        list_images = []
        backet_final = ''
        format_images_final = ''

        for backet in self.list_backet:

            for format_img in list_format_images:
                if len(id_product_str) > 8:
                    len_art = 6
                    len_min_art = 4
                else:
                    len_art = 5
                    len_min_art = 3

                url = 'https://{0}.wb.ru/vol{1}/part{2}/{3}/images/{4}/1.jpg'.format(backet,
                                                                                     id_product_str[:len_min_art],
                                                                                     id_product_str[:len_art],
                                                                                     id_product_str,
                                                                                     format_img
                                                                                     )
                response = requests.get(url)
                if response.status_code == 200:
                    status_code = 200
                    backet_final = backet
                    format_images_final = format_img
                    break

        if status_code == 200:
            path = self.__get_path(path_saller + '/{0}'.format(id_product_str))

            for i in range(1, 2):
                url = 'https://{0}.wb.ru/vol{1}/part{2}/{3}/images/{4}/{5}.jpg'.format(backet_final,
                                                                                       id_product_str[:len_min_art],
                                                                                       id_product_str[:len_art],
                                                                                       id_product_str,
                                                                                       format_images_final,
                                                                                       str(i))

                response = requests.get(url)
                if response.status_code == 200:
                    filename = path + '/{0}_{1}.jpg'.format(id_product_str, str(i))

                    with open(filename, 'wb') as imgfile:
                        imgfile.write(response.content)

                    list_images.append(filename)
                else:
                    break

        return list_images

    def __get_seller_id(self, url):
        response = requests.get(url=f"https://card.wb.ru/cards/detail?nm={self.__get_item_id(url=url)}")
        print(response.json())
        seller_id = Items.parse_obj(response.json()["data"])
        return seller_id.products[0].supplierId

    def parse(self):

        self.__save_csv()

    def __create_csv(self):
        with open("wb_data.csv", mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'название', 'цена', 'бренд', 'скидка', 'рейтинг', 'в наличии', 'id продавца',
                             'пути изображений товаров', 'хэштэги'])

    def __save_csv(self):

        _page = 1

        df_data = pd.DataFrame(
            columns=['date_load', 'id_product', 'name', 'price', 'brand', 'sale', 'rating', 'count_products',
                     'id_seller', 'path_img', 'hashtags', 'category_product', 'description', 'brandId'])

        date_load = datetime.now()
        date_load = str(date_load)[:16]

        while True:
            response = requests.get(
                f'https://catalog.wb.ru/sellers/catalog?dest=-1257786&supplier={self.seller_id}&page={_page}',
            )
            _page += 1
            print(response.json())
            items = Items.parse_obj(response.json()["data"])
            if not items.products:
                break

            id_saller = str(items.products[0].supplierId)
            path_saller = id_saller
            brandId = str(items.products[0].brandId)

            if not os.path.exists(path_saller):
                os.mkdir(path_saller)

            # with open(path_saller+"/wb_data_{0}.csv".format(self.seller_id), mode="a", newline="") as file:

            # writer = csv.writer(file)
            for product in items.products:
                print(product)
                try:
                    id_product = product.id
                    id_saller = product.supplierId
                    list_images = self.get_images(id_product, id_saller)
                    hashtag = self.__get_hashtages(id_product)
                    category_product, description = self.get_card_product(id_product, id_saller, date_load)

                    list_data = [date_load,
                                 id_product,
                                 product.name,
                                 product.salePriceU,
                                 product.brand,
                                 product.sale,
                                 product.rating,
                                 product.volume,
                                 id_saller,
                                 str(list_images),
                                 str(hashtag),
                                 category_product,
                                 description,
                                 brandId]

                    df_data.loc[len(df_data.index)] = list_data
                except Exception as e:
                    print(e)
        url = 'https://images.wbstatic.net/brands/small/{0}.jpg'.format(brandId)
        response = requests.get(url)

        if response.status_code == 200:
            if not os.path.exists(path_saller):
                os.mkdir(path_saller)
            filename = path_saller + '/logo.jpg'

            with open(filename, 'wb') as imgfile:
                imgfile.write(response.content)

        df_data.to_csv(path_saller + '/wb_data_{0}.csv'.format(id_saller), sep=';', encoding='utf-8')
        df_data.to_excel(path_saller + '/wb_data_{0}.xlsx'.format(id_saller))
        connection = sqlite3.connect(self.database.db_name)
        df_data.to_sql(name='products', con=connection, if_exists='append', index=False)


if __name__ == "__main__":
    date_start = datetime.now()

    # ParseWB("https://www.wildberries.ru/catalog/27605639/detail.aspx").parse()
    # ParseWB("https://www.wildberries.ru/catalog/158753519/detail.aspx").parse()
    # ParseWB('https://www.wildberries.ru/seller/887760').parse()

    import sqlite3

    connection = sqlite3.connect('aiseller.db')
    cursor = connection.cursor()
    cursor.execute("DROP TABLE card")
    connection.commit()
    cursor.execute("DROP TABLE products")
    connection.commit()
    connection.close()

    db_ = db.DataBase()
    db_.create_table_card()
    db_.create_table_products()

    # ParseWB('https://www.wildberries.ru/catalog/133680106/detail.aspx').parse()

    # ParseWB('https://www.wildberries.ru/catalog/158753519/detail.aspx').parse()
    print('Пошел дядя большой')
    date_start_dydy = datetime.now()
    ParseWB('https://www.wildberries.ru/catalog/15310995/detail.aspx').parse()
    print('Дядя отработал')
    print(datetime.now() - date_start)
    # cat = ParseWB('https://www.wildberries.ru/catalog/133680106/detail.aspx').get_card_product(133655304, 14238)
    # print(cat)
    print(datetime.now() - date_start)

# НУЖНО СДЕЛАТЬ
# ХЭШТЭГИ API https://search-tags.wildberries.ru/tags?nm_id=32761446 +
# PRICE HISTORY https://basket-03.wb.ru/vol327/part32761/32761446/info/price-history.json
# INFO SALLERS https://basket-03.wb.ru/vol327/part32761/32761446/info/sellers.json
# ОПИСАНИЕ ТОВАРА https://basket-10.wb.ru/vol1336/part133655/133655304/info/ru/card.json
# PICTERS https://basket-03.wb.ru/vol327/part32761/32761446/images/big/1.jpg +


"""'date_load', 'id_product', 'name', 'price', 'brand', 'sale', 'rating', 'count_products', 'id_seller',
                     'path_img', 'hashtags', 'category_product'"""

"""
import sqlite3

    connection = sqlite3.connect('aiseller.db')
    cursor = connection.cursor()
    cursor.execute("DROP TABLE card")
    connection.commit()
    cursor.execute("DROP TABLE products")
    connection.commit()
    connection.close()


cursor.execute('''CREATE TABLE IF NOT EXISTS products
                        (date_load TEXT, id_product INT, name TEXT, price REAL, brand TEXT, sale REAL, rating REAL,
                         count_products INT, id_seller INT, path_img TEXT, hashtags TEXT, category_product TEXT)''')

connection.commit()
    connection.close()

"""