import sqlite3
from datetime import datetime
from pytz import timezone
from json import loads, dumps
import pandas as pd

class DataBase:
    def __init__(self, db_name='aiseller.db'):
        "Инициализируем базу данных"
        self.db_name = db_name

    #___ CREAT TABLE ___

    def creat_table_temp(self):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        with connection:
            text = """ CREATE TABLE IF NOT EXISTS temp_parser ( user_id TEXT, url_wb TEXT, date_req TEXT, regions TEXT, 
                    gender TEXT, age REAL, budget TEXT)"""
            cursor.execute(text)
            cursor.close()

    def create_table_products(self):
        'Создаем таблицу запросов на продукты'
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        with connection:
            text = '''CREATE TABLE IF NOT EXISTS products
                        (date_load TEXT, id_product INT, name TEXT, price REAL, brand TEXT, sale REAL, rating REAL,
                         count_products INT, id_seller INT, path_img TEXT, hashtags TEXT, category_product TEXT,
                         description TEXT, brandId TEXT)'''
            cursor.execute(text)
            cursor.close()

    def create_table_card(self):
        'Создаем таблицу запросов на продукты'
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        with connection:
            text = '''CREATE TABLE IF NOT EXISTS card
                        (date_load TEXT, id_seller INT, id_product INT, category_product TEXT, description TEXT,
                        json_file TEXT )'''
            cursor.execute(text)
            cursor.close()

    #___ FUNCTION TABLE ___

    def add_value_card(self, date_load: str, id_seller:int, id_product: int, category_product: str, description: str,
                       json_file: str):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        with connection:
            text_q = "INSERT INTO card (date_load, id_seller, id_product, category_product, description, json_file) " \
                     "VALUES (?, ?, ?, ?, ?, ?)"
            cursor.execute(text_q, (date_load, id_seller, id_product, category_product, description,
                                         json_file)).fetchall()
            cursor.close()

    def add_value_temp_parser(self, user_id: str, url_wb: str, regions: str, gender: str, age: float,
                              budget: str) -> bool:

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        with connection:
            date_req = str(datetime.now(timezone('Europe/Moscow')))[:16]
            cursor.execute("""INSERT INTO 'temp_parser' (user_id, url_wb, date_req, regions, gender, age, budget) 
                            VALUES (?,?,?,?,?,?,?)""",
                           (user_id, url_wb, date_req, regions, gender, age, budget))
            cursor.close()

            return True

    def get_value_temp_parser(self):

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        with connection:
            result = cursor.execute("""SELECT * FROM 'temp_parser'""").fetchone()
            cursor.close()

            return result

    def delete_value_temp_parser(self, user_id: str, url_wb: str, date_req: str):

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        with connection:
            cursor.execute("""DELETE FROM 'temp_parser' WHERE user_id = ? and 
                              url_wb = ? and date_req = ?""", (user_id, url_wb, date_req)).fetchone()
            cursor.close()
            return True

#--------------- OLD ---------------------------
    def add(self, name_table: str, column_name: str, type_columns: str):
        self.cursor.execute(f"ALTER TABLE {name_table} add {column_name} {type_columns}")

    def delete_user(self, _id: int) -> None:
        "Удаляем запрос из таблицы"
        with self.connection:
            self.cursor.execute("DELETE FROM 'users' WHERE id = ?", (_id,))

    def check_user(self, user_id: int, table: str) -> bool:
        "Проверяем есть ли запись в таблице"
        with self.connection:
            return bool(len(self.cursor.execute(f"SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchall()))

    def create_users(self):
        'Создаем таблицу платежей'
        with self.connection:
            text = """CREATE TABLE IF NOT EXISTS users (id INTEGER primary key autoincrement, 
                user_id INTEGER NOT NULL, count_req INTEGER,
                reg_date TEXT, limit_ INTEGER, name TEXT, query TEXT, 
                subscribe_date_start TEXT, subscribe_date_end TEXT)"""
            self.cursor.execute(text)

    def creat_table_payments(self):

        'Создаем таблицу платежей'
        with self.connection:
            text = """CREATE TABLE IF NOT EXISTS payments (id INTEGER primary key autoincrement, user_id INTEGER NOT NULL, 
                    sum_pay real, payment_id TEXT, label_payment TEXT, subscription_link TEXT, date_req TEXT,
                    comment TEXT)"""
            self.cursor.execute(text)

    def new_payments(self, user_id: int, sum_pay: int, payment_id: str, label_payment: str,
                  subscription_link: str, date_req: str, comment: str):
        'Добавляем платеж в таблицу'
        with self.connection:
            text_q = "INSERT INTO orders (user_id, sum_pay, label_payment, payment_id, subscription_link, date_req, comment) " \
                     "VALUES (?, ?, ?, ?, ?, ?, ?)"
            self.cursor.execute(text_q,
                (user_id, sum_pay, label_payment, payment_id, subscription_link, date_req, comment)).fetchall()

    def creat_table_orders(self):

        'Создаем таблицу платежей'
        with self.connection:
            text = """CREATE TABLE IF NOT EXISTS orders (id INTEGER primary key autoincrement, user_id INTEGER NOT NULL, 
                    sum_pay real, payment_id TEXT, label_payment TEXT, subscription_link TEXT, date_req TEXT)"""
            self.cursor.execute(text)

    def creat_table_test_payment(self):

        'Создаем таблицу тестовых платежей'
        with self.connection:
            text = """CREATE TABLE IF NOT EXISTS test_payment (id INTEGER primary key autoincrement, user_id INTEGER NOT NULL, 
                    sum_pay real, is_test TEXT)"""
            self.cursor.execute(text)

    def get_test_payments(self, user_id: int):
        text = f"""select user_id from test_payment where user_id= {user_id} """
        with self.connection:
            return len(self.cursor.execute(text).fetchall())

    def new_test_payment_value(self, user_id: int, sum_pay: int, is_test: str):
        'Добавляем платеж в таблицу'
        with self.connection:
            text_q = "INSERT INTO test_payment (user_id, sum_pay, is_test) " \
                     "VALUES (?, ?, ?)"
            self.cursor.execute(text_q, (user_id, sum_pay, is_test)).fetchall()

    def new_order(self, user_id: int, sum_pay: int, payment_id: str, label_payment: str,
                  subscription_link: str, date_req: str):
        'Добавляем платеж в таблицу'
        with self.connection:
            text_q = "INSERT INTO orders (user_id, sum_pay, label_payment, payment_id, subscription_link, date_req) " \
                     "VALUES (?, ?, ?, ?, ?, ?)"
            self.cursor.execute(text_q,
                (user_id, sum_pay, label_payment, payment_id, subscription_link, date_req)).fetchall()

    def get_payment_id(self, user_id: int):
        text = """select payment_id from orders 
                where user_id= ? and datetime(date_req) = (select max(datetime(date_req)) from orders)"""
        with self.connection:
            return self.cursor.execute(text, (user_id,)).fetchall()

    def new_order_value(self, parameter: str, value: str, payment_id: str):
        'Обновляем платеж в таблице'
        with self.connection:
            self.cursor.execute(f"UPDATE 'orders' SET {parameter} = ? WHERE payment_id = ?", (value, payment_id))

    def new_user(self, user_id: int, name: str, query: str) -> bool:
        "Создаём нового пользователя"
        with self.connection:
            start_count_req = 1
            if not self.check_user(user_id, 'users'):
                self.cursor.execute("INSERT INTO 'users' (user_id, reg_date, count_req, limit_, name, query) VALUES (?,?,?,?,?,?)",
                                    (user_id, str(datetime.now(timezone('Europe/Moscow')))[:16], start_count_req, start_count_req, name,
                                     query,))
                return True
            else:
                return False

    def get_all_orders(self) -> list:
        "Получем список все заказы"
        with self.connection:
            return self.cursor.execute(f"select count( distinct user_id ) as count_user "
                                       f"from orders where response not null").fetchall()

    def get_all_users_not_buy(self) -> list:
        "Получем список всех пользователей"
        with self.connection:
            return self.cursor.execute(f"SELECT * FROM users where subscribe_date_start isnull").fetchall()

    def get_all_users(self) -> list:
        "Получем список всех пользователей"
        with self.connection:
            return self.cursor.execute(f"SELECT * FROM 'users'").fetchall()

    def get_users_whitout_sub(self) -> list:
        "Получем список всех пользователей"
        with self.connection:
            return self.cursor.execute(f"SELECT user_id FROM 'users' where subscribe_date_start is null ").fetchall()

    def create_request(self, user_id: int, prompt: str, mes_id: int, response_gpt: str, date_req: str) -> None:
        "Создаём запрос в таблице"
        with self.connection:
            self.cursor.execute("INSERT INTO 'req' (user_id, prompt, mes_id, response_gpt, date_req)"
                                " VALUES (?,?,?,?,?)",
                                (user_id, prompt, mes_id, response_gpt, date_req))

    def get_requests(self) -> list:
        "Получаем список всех запросов"
        with self.connection:
            return self.cursor.execute("SELECT * FROM 'req'").fetchall()

    def delete_request(self, _id: int) -> None:
        "Удаляем запрос из таблицы"
        with self.connection:
            self.cursor.execute("DELETE FROM 'req' WHERE id = ?", (_id,))

    def get_user_value(self, user_id: int, parameter: str, json: bool = False) -> any:
        "Получем значение пользователя"
        with self.connection:
            result = self.cursor.execute(f"SELECT {parameter} FROM 'users' WHERE user_id = ?", (user_id,)).fetchmany(1)
            if bool(len(result)) is False:
                return None
            elif json:
                return loads(result[0][0])
            else:
                return result[0][0]

    def new_user_value(self, user_id: int, parameter: str, value: any, json: bool = False) -> None:
        "Обновляем значения у пользователя"
        with self.connection:
            if json:
                value = dumps(value)
            self.cursor.execute(f"UPDATE 'users' SET {parameter} = ? WHERE user_id = ?", (value, user_id))

    def context(self, user_id: int, role: str = None, message: str = None) -> list:
        "Работа с контекстом"
        try:
            old_messages = self.get_user_value(user_id, 'context', True)
        except Exception:
            old_messages = []
        if len(old_messages) > 2:
            old_messages.pop(0)
        if role and message:
            new_message = {"role": role, "content": message}
            old_messages.append(new_message)
        self.new_user_value(user_id, 'context', old_messages, True)
        return old_messages

    def save_all_data_users(self):

        with self.connection:
            df = pd.DataFrame(self.cursor.execute(f"SELECT * FROM 'users'").fetchall())
            df.to_csv(r'data_users.csv')

    def save_all_data_req(self):

        with self.connection:
            df = pd.DataFrame(self.cursor.execute(f"SELECT * FROM 'req'").fetchall())
            df.to_csv(r'data_req.csv')

    def save_all_req_and_users(self):
        text = """select r.*, u.count_req, u.reg_date, u.subscribe_date_start, u.subscribe_date_end
                from req as r
                left join users as u
                on r.user_id = u.user_id"""

        with self.connection:
            df = pd.DataFrame(self.cursor.execute(text).fetchall())
            df.to_csv(r'data_req_and_users.csv')