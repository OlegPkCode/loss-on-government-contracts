import psycopg2
from psycopg2 import Error
import csv
import pprint
from lib_gz import *

'''
Создает в базе данных таблицу pni, затем заходит по пути в переменной path_to_data , находит файл (файлы) ods, 
конвертирует их в csv формат и заполняет из этих данных таблицу pni 
'''

try:
    # Подключиться базе данных gz
    connection = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    cursor = connection.cursor()

    if table_exist(cursor, 'pni'):
        # Удаляем все записи из таблицы
        cursor.execute("DELETE FROM pni;")
        connection.commit()
    else:
        # Создаем таблицу pni
        create_table = """CREATE TABLE IF NOT EXISTS pni (
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
        );"""
        cursor.execute(create_table)
        connection.commit()

    # Заполняем таблицу pni из одноименного csv файла
    convert_ods_to_csv('pni.csv')
    with open(path_to_data + 'pni.csv', 'r') as file:
        reader = csv.DictReader(file, delimiter=';')
        num = 1
        for row in reader:
            if len(row['sname']) > 0:
                print('Обработка строки - ', num)
                num += 1
                qty = replace_comma_to_dot(row['qty'])
                price = replace_comma_to_dot(row['price'])
                total = replace_comma_to_dot(row['total'])
                # Выполнение SQL-запроса для вставки данных в таблицу
                insert_query = f"""
                    INSERT INTO pni (sname, name, name_dop, qty, unit, price, total, contract, year, customer)
                    VALUES ('{row['sname']}', '{row['name']}', '{row['name_dop']}', '{qty}', '{row['unit']}', 
                    '{price}', '{total}', '{row['contract']}', '{row['year']}', '{row['customer']}');
                """
                cursor.execute(insert_query)
                connection.commit()

    # Выводим результат
    cursor.execute("SELECT * from pni;")
    record = cursor.fetchall()
    pprint.pprint(record)

except (Exception, Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    if connection:
        cursor.close()
        connection.close()
