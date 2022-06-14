from tapi_yandex_metrika import YandexMetrikaStats
import json
import pandas as pd
import config
import time
from datetime import date, timedelta
import datetime
import logging
import gspread
import sys

def get_needed_data(worksheet):
    '''Получает объект листа Google таблиц, из которого нужно получить данные. Возвращает кортеж из двух дат для выгрузки в формате строки. Первую считает как следующую за последней, которая уже есть в таблице с данными. Последнюю - вчера. Если период выгрузки отрицательный, генерирует SystemExit.'''

    values_list = worksheet.col_values(1)
    date1 = values_list[-1]
    date1 = datetime.datetime.strptime(date1, '%Y-%m-%d').date()
    date1 = date1 + timedelta(days=1)
    date2 = date.today() - timedelta(days=1)       # вчера
    date_diff = (date2 - date1).days
    if date_diff < 0:
        sys.exit('Нет новых данных для загрузки. Попробуйте завтра.')
    else:
        date1 = str(date1)
        date2 = str(date2)
        dates = (date1, date2)
        return dates

def import_data(api_request, params):
    '''Получает данные из Yandex.Metrika API. Возвращает массив результатов'''
    result = api_request.stats().get(params=params)
    result = result().data
    result = result[0]['data']
    return result

def parse_json(result):
    '''Парсит Json в list для Google Таблиц. Возвращает список списков для загрузки в Google Таблицы.'''
    values = []
    # headers = ['date', 'trafficSource', 'trafficSourceEngine', 'UTMCampaign', 'visits', 'users', 'chatMessage', 'cartOrder']
    # values.append(headers)                              # Добавляем строку с заголовками
    for i in range(len(result)-1):
        value = []
        for k in range(len(result[i]["dimensions"])):
            value.append(result[i]["dimensions"][k]["name"])
        for m in range(len(result[i]["metrics"])):
            value.append(result[i]["metrics"][m])
        values.append(value)
    return values
  
def main():
    start_time = time.time()

    # Импорт токена и счетчика
    ACCESS_TOKEN = config.token
    METRIC_IDS = config.counterId

    # Получение листа Google таблиц для работы
    gc = gspread.service_account(filename='google_key.json')
    sheet = gc.open_by_key(config.sheet)
    worksheet = sheet.worksheet(config.worksheet)

    # Получение начальной и конечной даты диапазона загрузки данных
    dates = get_needed_data(worksheet)
    
    #Параметры запроса для библиотеки tapi_yandex_metrika
    api = YandexMetrikaStats(
        access_token=ACCESS_TOKEN, 
        receive_all_data=True)                          # Если True, будет скачивать все части отчета. По умолчанию False.

    params = dict(
        ids = METRIC_IDS,
        metrics = "ym:s:visits,ym:s:users,ym:s:goal49436773reaches,ym:s:goal47702074reaches",
        dimensions = "ym:s:date,ym:s:lastsignTrafficSource,ym:s:lastsignSourceEngine,ym:s:UTMCampaign",
        date1 = dates[0],
        date2 = dates[1],
        sort = "ym:s:date",
        accuracy="full",
        limit = 2000)

    result = import_data(api, params)
    values = parse_json(result)
    worksheet.append_rows(values)
    
    end_time = time.time()
    total_time = round((end_time - start_time), 3)
    logging.info(f'Total time: {total_time} s.')

if __name__ == '__main__':
    main()
