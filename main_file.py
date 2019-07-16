from BigQuery import BigQuery
from YandexDirect import YandexDirect
import CallTouch
import pandas as pd
from yaml import load


if __name__ == '__main__':
    config = load(open('path to your yaml file', 'r'))
    # Загружаем необходимые параметры из yaml файла
    config = load(open('path to your yaml file', 'r'))
    # Определяем объекты
    yandex_direct = YandexDirect(config['yandex']['oauth']['client_login'], config['yandex']['oauth']['access_token'])
    big_query = BigQuery('your json file')
    calltouch = CallTouch()
    # Создаем Dataset в BigQuery
    big_query.create_dataset('your dataset name')
    # Создаем shema для таблицы BigQuery
    schema = calltouch.shema_for_bgq()
    # Создаем таблицу
    big_query.create_table('your dataset name', 'your table name', schema)
    # Получаем данные из Calltouch за определенный период
    total_result = calltouch.get_calls('date from', 'date to')
    # Готовим полученные данные для записи в BigQuery
    result = calltouch.to_tuples_for_insert_rows(total_result)
    # Записывем данные
    big_query.insert_rows('your dataset name', 'your table name', result)

    # Получаем список кампаний, их описание и отдельно список id
    campaign, campaign_ids = yandex_direct.get_campaigns()
    # Получаем список групп, их описание и отдельно список id
    group, group_ids = yandex_direct.get_groups(campaign_ids)
    # Получаем список объявлений и их описание
    ads = yandex_direct.get_ads(group_ids)
    # Получаем список ключевых слов и их описание
    keyword = yandex_direct.get_keywords(group_ids)
    # Получаем список таргетингов и аудиторий, а так же их описание
    retargeting = yandex_direct.get_retargeting_list(group_ids)
    # Меняем название столбцов в полученных Dataframe
    campaign = campaign.rename(columns={"Amount": "Дневной бюджет кампании", "Clicks": "Количество кликов кампании",
                                        "ClientInfo": "Информация о клиенте", "CounterIds": "Номера счетчиков метрики",
                                        "Currency": "Валюта", "DailyBudget": "Дневной бюджет кампании",
                                        "EndDate": "Дата окончания кампании", "Id": "CampaignId",
                                        "Impressions": "Количество показов кампании",
                                        "Mode": "Результаты модерации кампании", "Name": "Название кампании",
                                        "StartDate": "Дата старта кампании", "State": "Состояние кампании",
                                        "Status": "Статус кампании", "TimeZone": "Временная зона",
                                        "Type": "Тип кампании"})
    group = group.rename(columns={"Id": "AdGroupId", "Name": "Название группы", "RegionIds": "ID Регионов",
                                  "ServingStatus": "Статус возможности показов", "Status": "Статус группы",
                                  "Subtype": "Подтип группы", "Type": "Тип группы"})
    ads = ads.rename(
        columns={"AdCategories": "Особая категория", "DisplayUrlPath": "Отображаемая ссылка", "Href": "Ссылка",
                 "Id": "AdID", "Mobile": "Мобильное объявление", "State": "Состояние объявление",
                 "Status": "Статус объявления", "Subtype": "Подтип объявления", "Text": "Текст объявления",
                 "Title": "Заголовок 1", "Title2": "Заголовок 2", "Type": "Тип объявления"})
    keyword = keyword.rename(
        columns={"Id": "KeywordId", "Keyword": "Ключевое слово", "ServingStatus": "Статус возможности показов группы",
                 "State": "Состояние ключевого слова", "Status": "Статус ключевого слова"})
    # Объединяем все полученные Dataframe в один
    ads_dataframe = keyword.merge(ads, on=['AdGroupId', 'CampaignId'], how='left')
    group_dataframe = ads_dataframe.merge(group, on=["AdGroupId", "CampaignId"], how="left")
    campaign_dataframe = group_dataframe.merge(campaign, on="CampaignId", how="left")
    # Убираем utm метки и все параметры и оставляем только URL
    campaign_dataframe['Ссылка'] = campaign_dataframe['Ссылка'].apply(lambda x: str(x.split('?')[0]))
    # Записывем полученный Dataframe в excel
    writer = pd.ExcelWriter('example.xlsx')
    campaign_dataframe.to_excel(writer, 'Sheet1')