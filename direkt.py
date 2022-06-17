import logging
import time
import gspread
import config
from functions import *


def main():
    start_time = time.time()

    # Получение листа Google таблиц для работы
    gc = gspread.service_account(filename='google_key.json')
    sheet = gc.open_by_key(config.sheet)
    worksheet = sheet.worksheet(config.worksheet_direkt)

    # Получение начальной и конечной даты диапазона загрузки данных
    dates = get_needed_data(worksheet)

    # Импорт доступов
    token = config.token_direkt
    client_login = config.client_login

    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": dates[0],
                "DateTo": dates[1]
            },
            "FieldNames": [
                "Date",
                "CampaignName",
                'CampaignId',
                "Impressions",
                "Clicks",
                "Cost"
            ],
            "ReportName": "НАЗВАНИЕ_ОТЧЕТА",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    import_direkt_data(token, client_login, body)

    end_time = time.time()
    total_time = round((end_time - start_time), 3)
    logging.info(f'Total time: {total_time} s.')


if __name__ == '__main__':
    main()
