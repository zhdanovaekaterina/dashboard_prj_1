from tapi_yandex_metrika import YandexMetrikaStats
import time
import logging
import gspread
import config
from functions import *


def main():
    start_time = time.time()

    # Импорт токена и счетчика
    access_token = config.token
    metric_ids = config.counterId

    # Получение листа Google таблиц для работы
    gc = gspread.service_account(filename='google_key.json')
    sheet = gc.open_by_key(config.sheet)
    worksheet = sheet.worksheet(config.worksheet_metrika)

    # Получение начальной и конечной даты диапазона загрузки данных
    dates = get_needed_data(worksheet)
    
    # Параметры запроса для библиотеки tapi_yandex_metrika
    api = YandexMetrikaStats(
        access_token=access_token,
        receive_all_data=True)                  # Если True, будет скачивать все части отчета. По умолчанию False.

    params = dict(
        ids=metric_ids,
        metrics="ym:s:visits,ym:s:users,ym:s:goal49436773reaches,ym:s:goal47702074reaches",
        dimensions="ym:s:date,ym:s:lastsignTrafficSource,ym:s:lastsignSourceEngine,ym:s:UTMCampaign",
        date1=dates[0],
        date2=dates[1],
        sort="ym:s:date",
        accuracy="full",
        limit=2000)

    # Заголовки таблицы - формируются самостоятельно, если импорт данных впервые.
    # Необязательный параметр для функции parse_metrika_json_tolist().
    # Сначала перечисляются dimensions, затем metrics из набора параметров.
    headers = ['date', 'trafficSource', 'trafficSourceEngine', 'UTMCampaign', 'visits', 'users', 'chatMessage',
               'cartOrder']

    # Получаем данные из Метрики
    result = import_metrika_data(api, params)

    # Парсим массив данных в список списков для загрузки в Google таблицы
    if dates[2]:
        values = parse_metrika_json_tolist(result, headers)
    else:
        values = parse_metrika_json_tolist(result)

    # Загружаем данные в Google таблицы
    worksheet.append_rows(values)
    
    end_time = time.time()
    total_time = round((end_time - start_time), 3)
    logging.info(f'Total time: {total_time} s.')


if __name__ == '__main__':
    main()
