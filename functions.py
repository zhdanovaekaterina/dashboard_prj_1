from tapi_yandex_metrika import YandexMetrikaStats
from datetime import date, timedelta
import datetime
import sys
import json
import requests
from requests.exceptions import ConnectionError
from time import sleep


def get_needed_data(worksheet):
    """Получает объект листа Google таблиц, из которого нужно получить данные.
    Возвращает кортеж из двух дат для выгрузки в формате строки.
    Первую считает как следующую за последней, которая уже есть в таблице с данными. Последнюю - вчера.
    Если все данные за вчера и ранее уже загружены, генерирует SystemExit."""

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


def import_metrika_data(api_request, params):
    """Получает данные из Yandex.Metrika API. Возвращает массив результатов."""
    result = api_request.stats().get(params=params)
    result = result().data
    result = result[0]['data']
    return result


def import_direkt_data(token, client_login, body):
    """Получает данные из Директа.
    Params -
    token: токен доступа;
    client_login: логин клиента;
    body: тело запроса."""

    reports_url = 'https://api.direct.yandex.com/json/v5/reports'

    # --- Подготовка запроса ---
    # Создание HTTP-заголовков запроса
    headers = {
        "Authorization": "Bearer " + token,
        "Client-Login": client_login,
        "Accept-Language": "ru",
        "processingMode": "auto",
        "skipReportHeader": "true",
        "skipReportSummary": "true"
    }

    # Создание тела запроса
    body = body
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
                print("Содержание отчета: \n{}".format(req.text))
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
        values.append(headers)                              # Добавляем строку с заголовками, если она передана
    for i in range(len(result)-1):
        value = []
        for k in range(len(result[i]["dimensions"])):
            value.append(result[i]["dimensions"][k]["name"])
        for m in range(len(result[i]["metrics"])):
            value.append(result[i]["metrics"][m])
        values.append(value)
    return values


if __name__ == '__main__':
    pass
