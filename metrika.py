from tapi_yandex_metrika import YandexMetrikaStats
import json
import pandas as pd
import config
import time
from datetime import date, timedelta
import logging
import gspread

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

def to_google_sheets(data):
    '''Выгрузка данных в Google Таблицы. Работает, если необходимо добавить меньше 100 строк!'''
    gc = gspread.service_account(filename='google_key.json')
    sheet = gc.open_by_key(config.sheet)
    worksheet = sheet.worksheet(config.worksheet)
    worksheet.append_rows(data)

def main():
    start_time = time.time()

    # Импорт токена и счетчика
    ACCESS_TOKEN = config.token
    METRIC_IDS = config.counterId

    # Задание периода выгрузки
    # TODO: задавать период со следующего дня после того, который последний на листе по вчера; добавить выгрузку последней даты и проверку, что timedelta положительна (иначе, что последняя дата - не вчера)
    date1 = str(date.today() - timedelta(days=1))       # вчера
    date2 = str(date.today() - timedelta(days=1))       # вчера

    #Параметры запроса для библиотеки tapi_yandex_metrika
    api = YandexMetrikaStats(
        access_token=ACCESS_TOKEN, 
        receive_all_data=True)                          # Если True, будет скачивать все части отчета. По умолчанию False.

    params = dict(
        ids = METRIC_IDS,
        metrics = "ym:s:visits,ym:s:users,ym:s:goal49436773reaches,ym:s:goal47702074reaches",
        dimensions = "ym:s:date,ym:s:lastsignTrafficSource,ym:s:lastsignSourceEngine,ym:s:UTMCampaign",
        date1 = date1,
        date2 = date2,
        sort = "ym:s:date",
        accuracy="full",
        limit = 2000)

    result = import_data(api, params)
    values = parse_json(result)
    to_google_sheets(values)
    
    end_time = time.time()
    total_time = round((end_time - start_time), 3)
    logging.info(f'Total time: {total_time} s.')

if __name__ == '__main__':
    main()
