'''
Создает в базе данных таблицу products, затем заходит по пути в переменной data_path, берет все файлы *.ods,
конвертирует их в csv формат, формирует итоговый csv файл и заполняет этими данными таблицу products.
'''

import psycopg2
from psycopg2 import Error
import csv
from lib_gz import *

file_output = data_path + 'products.csv'

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

    # Если таблица существует - удаляем ее
    cursor.execute("DROP TABLE IF EXISTS products;")
    connection.commit()

    # Создаем таблицу products
    sql = """CREATE TABLE products (
        sname VARCHAR(50),
        name VARCHAR(2000),
        name_dop VARCHAR(500),
        qty REAL,
        unit VARCHAR(30),
        price REAL,
        total REAL,
        contract VARCHAR(30),
        year INTEGER,
        customer VARCHAR(500),
        find_text VARCHAR(30)
    );"""
    cursor.execute(sql)
    connection.commit()
    print('Таблица products успешно создана в PostgreSQL')

    # Заполняем таблицу products из одноименного csv файла
    convert_ods_to_csv(file_output)
    with open(file_output, 'r') as file:
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
                    INSERT INTO products (sname, name, name_dop, qty, unit, price, total, contract, year, customer)
                    VALUES ('{row['sname'].strip()}', '{row['name']}', '{row['name_dop']}', '{qty}', '{row['unit']}', '{price}',
                    '{total}', '{row['contract']}', '{row['year']}', '{row['customer']}');
                """
                cursor.execute(insert_query)
                connection.commit()

except (Exception, Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    if connection:
        cursor.close()
        connection.close()
