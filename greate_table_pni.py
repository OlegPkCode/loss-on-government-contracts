import psycopg2
from psycopg2 import Error
import csv
import pprint
from config import path_to_data

def table_exist(table_name):
    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';")
    record = cursor.fetchall()
    return len(record) == 1


try:
    # Подключиться базе данных gz
    connection = psycopg2.connect(
        host = '127.0.0.1',
        port = '5432',
        database = 'gz',
        user = 'olejonlm',
        password = '111'
    )
    cursor = connection.cursor()

    if table_exist('pni'):
        # Удаляем записи с таблицы
        cursor.execute("DELETE FROM pni")
        connection.commit()
    else:
        # Создаем таблицу pni
        create_table = '''CREATE TABLE IF NOT EXISTS pni (
            sname VARCHAR(50),
            name VARCHAR(500),
            name_dop VARCHAR(500),
            qty REAL,
            unit VARCHAR(30),
            price REAL,
            total REAL,
            contract VARCHAR(30),
            year INTEGER,
            customer VARCHAR(500)
        )'''
        cursor.execute(create_table)
        connection.commit()

    # Заполняем таблицу pni из одноименного csv файла
    with open(path_to_data + 'pni.csv', 'r') as file:
        reader = csv.DictReader(file, delimiter=';')
        num = 1
        for row in reader:
            if len(row['sname']) > 0:
                print('Обработка строка - ', num)
                num += 1
                qty = row['qty'].replace(',', '.')
                price = row['price'].replace(',', '.')
                total = row['total'].replace(',', '.')
                # Выполнение SQL-запроса для вставки данных в таблицу
                insert_query = f'''
                    INSERT INTO pni (sname, name, name_dop, qty, unit, price, total, contract, year, customer)
                    VALUES ('{row['sname']}', '{row['name']}', '{row['name_dop']}', '{qty}', '{row['unit']}', '{price}', '{total}', '{row['contract']}', '{row['year']}', '{row['customer']}');
                '''
                cursor.execute(insert_query)
                connection.commit()

    # Получить результат
    cursor.execute('SELECT * from pni')
    record = cursor.fetchall()
    pprint.pprint(record)

except (Exception, Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print('Соединение с PostgreSQL закрыто')
