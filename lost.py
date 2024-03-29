'''
Формируем данные предполагаемого ущерба. Для каждой позиции из таблицы customers смотрим закупочные цены у других заказчиков.
На основе этих данных рассчитываем справедливую цену и выводим предполагаемый ущерб.
Результат записываем в файл file_output
'''

import psycopg2
from psycopg2 import Error
from pprint import pprint
import csv
from lib_gz import data_path, host, port, database, user, password, replace_dot

file_output = data_path + 'lost.csv'


# Делаем выборку позиций, купленых по меньшей цене, чем заданная и расчитываем среднюю цену из этой выборки.
# Возвращаем среднюю цену и все цени и контракты, которые ниже этой средней (справедливой) цены
def get_true_price(sname, price, year):
    cursor.execute(
        f"SELECT price, contract FROM products  WHERE sname = '{sname}' AND price < {price - 0.01} AND year = {year} ORDER BY price;")
    record = cursor.fetchall()
    if len(record) == 0:
        res = ('', '', '')
    else:
        base = ''
        sum = 0
        count = 0
        for item in record:
            count += 1
            sum += item[0]
            base = base + str(item[0]) + ' - ' + item[1] + ' / '
        res = (round(sum / count, 2), base, count)

    return res


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

    # Проверка на опечатки.
    cursor.execute("SELECT DISTINCT sname FROM customer WHERE sname <> '';")
    records = cursor.fetchall()
    sname_customer = {i[0] for i in records}

    cursor.execute("SELECT DISTINCT sname FROM products WHERE sname <> '';")
    records = cursor.fetchall()
    sname_products = {i[0] for i in records}

    pprint(sname_customer - sname_products)
    pprint(sname_products - sname_customer)

    # Формируем список словарей из таблицы customer, где заполнено короткое наименование
    cursor.execute("SELECT * FROM customer WHERE sname <> '';")
    record = cursor.fetchall()
    list_customer = []

    # Для каждой позиции из этого списка дополняем данные о справедливой цене, обоснования этой цени и предполагаемого ущерба
    for sname, name, name_dop, qty, unit, price, total, contract, year, customer in record:
        price_true, base, base_count = get_true_price(sname, price, year)
        if price_true == '':
            lost = ''
        else:
            lost = round(total - (qty * price_true))
        list_customer.append(
            {'sname': sname, 'name': name, 'name_dop': name_dop, 'qty': qty, 'unit': unit,
             'price': price, 'total': total, 'contract': contract, 'year': year, 'customer': customer,
             'price_true': price_true, 'base_count': base_count, 'base': base, 'lost': lost})

    list_customer.sort(key=lambda x: (x['year'], x['name']))

    # Записываем результат в csv файл
    with open(file_output, 'w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(('sname', 'name', 'name_dop', 'qty', 'unit', 'price', 'total', 'contract', 'year', 'customer',
                         'price_true', 'base_count', 'base', 'lost'))
        for item in list_customer:
            writer.writerow(
                (item['sname'], item['name'], item['name_dop'], replace_dot(item['qty']), item['unit'],
                 replace_dot(item['price']), replace_dot(item['total']),
                 item['contract'], item['year'], item['customer'], replace_dot(item['price_true']),
                 item['base_count'], item['base'],
                 replace_dot(item['lost'])))

except (Exception, Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print('Соединение с PostgreSQL закрыто')
