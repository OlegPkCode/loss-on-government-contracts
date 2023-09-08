import pandas as pd
import os

data_path = './data/'
host = '172.17.0.2'
port = '5432'
database = 'gz_db'
user = 'olejon'
password = '111'


# Функция конвертирует все файлы ods в каталоге data_path в файлы csv и формирует итоговый файл file_name в формате csv
def convert_ods_to_csv(file_output):
    list_files_ods = [i for i in os.listdir(data_path) if i.endswith('.ods')]
    list_files_ods.sort()

    with open(file_output, 'w') as f:
        for item in list_files_ods:
            print('Обработка файла', item)
            file_csv = data_path + item[:-4] + '.csv'
            read_file = pd.read_excel(data_path + item)
            read_file.to_csv(file_csv, index=False, sep=';', header=(lambda *x: list_files_ods.index(item) == 0)())
            s = open(file_csv).read()
            f.write(s)


# Возвращаем Истину, если таблица table_name существует в базе данных
def table_exist(cursor, table_name):
    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';")
    return len(cursor.fetchall()) == 1


def replace_comma(num):
    return str(num).replace(',', '.')


def replace_dot(num):
    return str(num).replace('.', ',')
