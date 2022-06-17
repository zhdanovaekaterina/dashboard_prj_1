from tapi_yandex_metrika import YandexMetrikaStats
from datetime import date, timedelta
import datetime
import sys
import json
import requests
from requests.exceptions import ConnectionError
from time import sleep
import csv
import re
import pandas as pd


def get_needed_data(worksheet):
    """Получает объект листа Google таблиц, из которого нужно получить данные.
    Возвращает кортеж из двух дат для выгрузки в формате строки и значение флага "Первая загрузка данных".
    Первую считает как следующую за последней, которая уже есть в таблице с данными. Последнюю - вчера.
    Если все данные за вчера и ранее уже загружены, генерирует SystemExit.
    Если данных в таблице нет, просит ввести начало диапазона с клавиатуры."""

    try:
        values_list = worksheet.col_values(1)
        date1 = values_list[-1]
        date1 = datetime.datetime.strptime(date1, '%Y-%m-%d').date()
        date1 = date1 + timedelta(days=1)
        is_first_data = False
    except IndexError:
        date1 = input('Введите дату, с которой необходимо начать сбор данных в формате YYYY-MM-DD: ')
        date1 = datetime.datetime.strptime(date1, '%Y-%m-%d').date()
        is_first_data = True

    date2 = date.today() - timedelta(days=1)  # вчера
    date_diff = (date2 - date1).days
    if date_diff < 0:
        sys.exit('Нет новых данных для загрузки. Попробуйте завтра.')
    else:
        date1 = str(date1)
        date2 = str(date2)
        dates = (date1, date2, is_first_data)
        return dates


def import_metrika_data(api_request, params):
    """Получает данные из Yandex.Metrika API. Возвращает массив результатов."""
    result = api_request.stats().get(params=params)
    result = result().data
    result = result[0]['data']
    return result


def import_direkt_data(token, dates: tuple, field_names: list, client_login=None):
    """Получает данные из Директа. Возвращает массив данных отчета.
    Params -
    token: токен доступа;
    client_login: логин клиента;
    dates: кортеж из дат начала и конца диапазона;
    field_names: список необходимых полей для выгрузки."""

    reports_url = 'https://api.direct.yandex.com/json/v5/reports'

    # --- Подготовка запроса ---
    # Создание HTTP-заголовков запроса
    headers = {
        "Authorization": "Bearer " + token,
        "Accept-Language": "ru",
        "processingMode": "offline",
        "returnMoneyInMicros": "false",
        "skipReportHeader": "true",
        "skipReportSummary": "true"
    }
    if client_login is not None:                            # Добавление логина к заголовкам, если он передан
        headers['Client-Login'] = client_login

    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": dates[0],
                "DateTo": dates[1]
            },
            "FieldNames": field_names,
            "ReportName": f"ОТЧЕТ{dates[0]}_{dates[1]}",
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    body = json.dumps(body, indent=4)

    # --- Запуск цикла для выполнения запросов ---
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросы
    while True:
        try:
            req = requests.post(reports_url, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break
            elif req.status_code == 200:
                print("Отчет создан успешно")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                return req.text
                break
            elif req.status_code == 201:
                print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print(
                    "Пожалуйста, попробуйте изменить параметры запроса:"
                    "уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(req.json()))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            break


def parse_metrika_json_tolist(result, headers=None):
    """Парсит Json-ответ Метрики в list для Google Таблиц. Возвращает список списков для загрузки в Google Таблицы."""
    values = []
    if headers is not None:
        values.append(headers)  # Добавляем строку с заголовками, если она передана
    for i in range(len(result) - 1):
        value = []
        for k in range(len(result[i]["dimensions"])):
            value.append(result[i]["dimensions"][k]["name"])
        for m in range(len(result[i]["metrics"])):
            value.append(result[i]["metrics"][m])
        values.append(value)
    return values


def group_data(data, headers=None):
    """Группирует данные по переданным параметрам. Возвращает список списков для загрузки в Google Таблицы."""
    df = pd.DataFrame(data)
    df.groupby(['0', '2', '3', '4'], axis=1).sum()
    print(df)


def parse_direkt_tsv_tolist(result, headers=None):
    """Парсит TSV-ответ Директа в list для Google Таблиц. Возвращает список списков для загрузки в Google Таблицы."""
    values = result.split('\n')
    values_new = []
    for i in range(len(values)):
        value = values[i].split('\t')
        values_new.append(value)
    if headers is None:                             # Удаляем из масива данных заголовки, если они уже есть
        del values_new[0]
    return values_new


if __name__ == '__main__':
    pass
