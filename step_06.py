import pprint

import psycopg2
from psycopg2 import Error
import csv


# import pprint

# from config import path_to_data

def get_true_price(sname, price, year):
    cursor.execute(
        f"SELECT price, contract FROM products  WHERE sname = '{sname}' AND price < {price - 0.01} AND year = {year} ORDER BY price;")
    record = cursor.fetchall()
    if len(record) == 0:
        res =  ('', '')
    else:
        base = ''
        sum = 0
        count = 0
        for item in record:
            count += 1
            sum += item[0]
            base = base + str(item[0]) + ' - ' + item[1] + ' / '
        res = (round(sum / count, 2), base)

    return res


try:
    # Подключиться базе данных gz
    connection = psycopg2.connect(
        host='127.0.0.1',
        port='5432',
        database='gz',
        user='olejonlm',
        password='111'
    )
    cursor = connection.cursor()

    # Формируем список (словарей) из таблицы pni, где заполнено короткое наименование
    cursor.execute("SELECT * FROM pni WHERE sname <> '';")
    record = cursor.fetchall()
    list_pni = []
    for item in record:
        price_true, base = get_true_price(item[0], item[5], item[8])
        if price_true != '':
            list_pni.append(
                {'sname': item[0], 'name': item[1], 'name_dop': item[2], 'qty': item[3], 'unit': item[4],
                 'price': item[5],
                 'total': item[6], 'contract': item[7], 'year': item[8], 'customer': item[9], 'price_true': price_true,
                 'base': base,
                 'loss': round(item[6] - (item[3] * price_true))})

    # print(list_pni)

    # res = get_true_price('треска бг', 346.55, 2018)
    # print(res)

    # Записываем его в csv файл
    with open('list_pni.csv', 'w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        for item in list_pni:
            writer.writerow(
                (item['sname'], item['name'], item['name_dop'], item['qty'], item['unit'], item['price'], item['total'],
                 item['contract'], item['year'], item['customer'], item['price_true'], item['base'], item['loss']))

except (Exception, Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    if connection:
        cursor.close()
        connection.close()
        # print('Соединение с PostgreSQL закрыто')
