import pandas as pd
import os

data_path = './data/'
host = '172.17.0.2'
port = '5432'
database = 'gz_db'
user = 'olejon'
password = '111'

# sqlite_db = 'gz.sqlite3'



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




# # Функция конвертирует все файлы ods в каталоге path_to_data в файлы csv и формирует итоговый файл file_name в формате csv
# def convert_ods_to_csv(file_name):
#     ls = [i for i in os.listdir(path_to_data) if i.endswith('.ods')]
#     ls.sort()
#
#     with open(path_to_data + file_name, 'w') as f:
#         for item in ls:
#             print('Обработка файла', item)
#             file_csv = path_to_data + item[:-4] + '.csv'
#             read_file = pd.read_excel(path_to_data + item)
#             read_file.to_csv(file_csv, index=False, sep=';', header=(lambda *x: ls.index(item) == 0)())
#             s = open(file_csv).read()
#             f.write(s)


# Возвращаем Истину, если таблица table_name существует в базе данных
def table_exist(cursor, table_name):
    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';")
    record = cursor.fetchall()
    return len(record) == 1



# Заменяет запятую на точку в переданном числе
def replace_comma_to_dot(num):
    num = str(num).replace(',', '.')
    return num


# Заменяет точку на запятую в переданном числе
def replace_dot_to_comma(num):
    num = str(num).replace('.', ',')
    return num