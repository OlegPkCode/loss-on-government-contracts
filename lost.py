import psycopg2
from psycopg2 import Error
import csv
from lib_gz import *

# Делаем выборку позиций, купленых по меньшей цене, чем заданная и расчитываем среднюю цену из этой выборки.
# Возвращаем среднюю цену и все цени и контракты, которые ниже этой средней (справедливой) цены
def get_true_price(sname, price, year):
    cursor.execute(
        f"SELECT price, contract FROM products  WHERE sname = '{sname}' AND price < {price - 0.01} AND year = {year} ORDER BY price;")
    record = cursor.fetchall()
    if len(record) == 0:
        res = ('', '')
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
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    cursor = connection.cursor()

    # Формируем список (словарей) из таблицы pni, где заполнено короткое наименование
    cursor.execute("SELECT * FROM pni WHERE sname <> '';")
    record = cursor.fetchall()
    list_pni = []
    # Для каждой позиции из этого списка дополняем данные о справедливой цене, обоснования этой цени и предполагаемого ущерба
    for item in record:
        price_true, base = get_true_price(item[0], item[5], item[8])
        if price_true == '':
            lost = ''
        else:
            lost = round(item[6] - (item[3] * price_true))
        list_pni.append(
            {'sname': item[0], 'name': item[1], 'name_dop': item[2], 'qty': item[3], 'unit': item[4],
             'price': item[5], 'total': item[6], 'contract': item[7], 'year': item[8], 'customer': item[9],
             'price_true': price_true, 'base': base, 'lost': lost})

    # Записываем результат в csv файл
    with open(path_to_data + 'lost.csv', 'w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(('sname', 'name', 'name_dop', 'qty', 'unit', 'price', 'total', 'contract', 'year', 'customer',
                         'price_true', 'base', 'lost'))
        for item in list_pni:
            writer.writerow(
                (item['sname'], item['name'], item['name_dop'], replace_dot_to_comma(item['qty']), item['unit'],
                 replace_dot_to_comma(item['price']), replace_dot_to_comma(item['total']),
                 item['contract'], item['year'], item['customer'], replace_dot_to_comma(item['price_true']), item['base'],
                 replace_dot_to_comma(item['lost'])))

except (Exception, Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print('Соединение с PostgreSQL закрыто')
