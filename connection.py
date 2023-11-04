import json
import logging
import os

from psycopg import connect

FOLDER_LOG = 'log'
if not os.path.exists(FOLDER_LOG):
    os.mkdir(FOLDER_LOG)
logging.basicConfig(
    level=logging.DEBUG,
    filename=(f"{FOLDER_LOG}//database.log"),
    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',)


class DataBase:
    data = None
    rows = None

    def auto_close(func):
        def wrapper(*args, **kwargs):
            self = args[0]
            try:
                self.conn = connect(dbname=self.db, user=self.user,
                                    password=self.password, host=self.host, port=self.port)
                self.cursor = self.conn.cursor()
                logging.info(f'Successfully connected to database {self.db}')
                func(*args, **kwargs)
                self.conn.commit()
                self.cursor.close()
                self.conn.close()
            except Exception as e:
                logging.error(f'Got an ERROR <<{e}>> with connecting with database in {func}')

        return wrapper

    def __init__(self, user_id, user_name, user='test_user', host='localhost', port='5432', db='test_db', password=''):
        self.user_id = user_id
        self.user_name = user_name
        self.user = user
        self.host = host
        self.port = port
        self.db = db
        self.password=password

    @auto_close
    def create_table(self):
        table_name = 'users'
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} '
                            f'(user_id INT PRIMARY KEY NOT NULL,'
                            f'user_name CHAR(70) NOT NULL,'
                            f'user_message_author CHAR(255))')
        logging.info(f'Table {table_name} was created successfully ')

    @auto_close
    def add_info_into_bd(self):
        self.cursor.execute(f'INSERT INTO users (user_id, user_name) '
                            f'VALUES(%s,%s) ', (self.user_id, self.user_name))
        logging.info(f'Successfully inserted  {(self.user_id, self.user_name)}')

    @auto_close
    def add_message_into_bd(self, data):
        column = 'user_message_author'
        self.cursor.execute(f'UPDATE users SET {column}=%s WHERE user_id=%s',
                            ((data), self.user_id))
        logging.info(f'Successfully updated  {column} and added {data} where user_id ={self.user_id}')

    @auto_close
    def select_data_from_db(self):
        global data
        self.cursor.execute(f'SELECT user_message_author FROM users WHERE user_id=%s',
                            (self.user_id,))
        self.data = json.dumps(self.cursor.fetchall(), indent=4)
        logging.info(f'Successfully got data where user_id ={self.user_id}')
        return self.data

    @auto_close
    def select_rows_from_db(self):
        global rows
        self.cursor.execute(f'SELECT * FROM users')
        self.rows = json.dumps(self.cursor.fetchall(), indent=4)
        logging.info(f'Successfully got data from the table')
        return self.rows

    @auto_close
    def delete_from_db(self, user_id):
        self.cursor.execute(f'DELETE FROM users WHERE user_id = {user_id}')
        logging.info(f'Successfully deleted data from the table where user_id={user_id}')


if __name__ == '__main__':
    a = DataBase(1, 'var')
    # a.select_data_from_db
    # a.add_info_into_bd()
    # a.add_message_into_bd('heeey')
    # a.select_data_from_db()
    # print(a.data)
    # a.select_rows_from_db()
    # print(a.rows)
    a.delete_from_db(1)
