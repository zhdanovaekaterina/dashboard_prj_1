from tapi_yandex_metrika import YandexMetrikaStats
import config_2 as config
import time
import logging
import gspread
from functions import *
import pprint


def main_metrika():
    start_time = time.time()

    # Импорт токена и счетчика
    access_token = config.token
    metric_ids = config.counterId

    # Задаем путь для ключей
    path = r'C:\Users\nasedkina\Desktop\Docs\Programming\dashboards_connector\keys\google_key_2.json'

    # Получение листа Google таблиц для работы
    gc = gspread.service_account(filename=path)
    sheet = gc.open_by_key(config.sheet)
    worksheet = sheet.worksheet(config.worksheet_metrika)

    # Получение начальной и конечной даты диапазона загрузки данных
    dates = get_needed_data(worksheet)
    if dates is None:
        return None

    # Параметры запроса для библиотеки tapi_yandex_metrika
    api = YandexMetrikaStats(
        access_token=access_token,
        receive_all_data=True)                       # Если True, будет скачивать все части отчета. По умолчанию False.

    params = dict(
        ids=metric_ids,
        metrics="ym:s:users,ym:s:goal229705038reaches,ym:s:goal131047114reaches,ym:s:goal35579934reaches",
        dimensions="ym:s:date,ym:s:clientID,ym:s:lastTrafficSource,ym:s:lastSearchEngine,ym:s:UTMSource,"
                   "ym:s:UTMCampaign",
        date1=dates[0],
        date2=dates[1],
        sort="ym:s:date",
        accuracy="full",
        limit=10000)

    # Заголовки таблицы - формируются самостоятельно, если импорт данных впервые.
    # Необязательный параметр для функции parse_metrika_json_tolist().
    # Сначала перечисляются dimensions, затем metrics из набора параметров.
    headers = ['date', 'clientID', 'trafficSource', 'trafficSearchEngine', 'UTMSource', 'UTMCampaign',
               'users', 'call', 'email', 'form']
    headers_2 = ['date', 'clientID', 'trafficSource', 'trafficSourceEngine', 'UTMCampaign',
               'users', 'call', 'email', 'form', 'anyGoal']

    # Получаем данные из Метрики
    result = import_metrika_data(api, params)

    # Парсит массив данных и возвращает сгруппированный список списков для загрузки в DataFrame и группировки
    if dates[2]:
        values = parse_metrika_json_tolist(result, headers)
    else:
        values = parse_metrika_json_tolist(result)

    # Указываем столбцы для группировки и группируем данные
    col_to_group = ['date', 'trafficSource', 'trafficSourceEngine', 'UTMCampaign']
    grouped_values = group_data(values, headers_2, col_to_group).values.tolist()

    # Загружаем данные в Google таблицы
    worksheet.append_rows(grouped_values)

    # Загружаем данные в Excel
    # grouped_values.to_excel('metrika_2.xlsx', index=False)

    end_time = time.time()
    total_time = round((end_time - start_time), 3)
    logging.info(f'Total time: {total_time} s.')


if __name__ == '__main__':
    main_metrika()
