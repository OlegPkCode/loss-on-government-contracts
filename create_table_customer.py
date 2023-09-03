'''
Создает в базе данных таблицу customer, затем заходит по пути в переменной data_path, берет все файлы *.ods,
конвертирует их в csv формат, формирует итоговый csv файл и заполняет этими данными таблицу customer.
'''

import psycopg2
import csv
import pprint
from lib_gz import *

file_output = data_path + 'customer.csv'

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
    cursor.execute("DROP TABLE IF EXISTS customer;")
    connection.commit()

    sql = """CREATE TABLE customer (
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
    cursor.execute(sql)
    connection.commit()

    # Заполняем таблицу customer из одноименного csv файла
    convert_ods_to_csv(file_output)
    print('111')
    with open(file_output, 'r') as file:
        reader = csv.DictReader(file, delimiter=';')
        num = 1
        for row in reader:
            # print(row)
            if len(row['sname']) > 0:
                print('Обработка строки - ', num)
                num += 1
                qty = replace_comma_to_dot(row['qty'])
                price = replace_comma_to_dot(row['price'])
                total = replace_comma_to_dot(row['total'])
                # Выполнение SQL-запроса для вставки данных в таблицу
                insert_query = f"""
                    INSERT INTO customer (sname, name, name_dop, qty, unit, price, total, contract, year, customer)
                    VALUES ('{row['sname']}', '{row['name']}', '{row['name_dop']}', '{qty}', '{row['unit']}',
                    '{price}', '{total}', '{row['contract']}', '{row['year']}', '{row['customer']}');
                """
                cursor.execute(insert_query)
                connection.commit()

    # Выводим результат
    cursor.execute("SELECT * from customer;")
    record = cursor.fetchall()
    pprint.pprint(record)

except (Exception, psycopg2.Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    if connection:
        cursor.close()
        connection.close()
