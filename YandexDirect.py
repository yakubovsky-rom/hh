import pandas as pd
import requests, json
import sys

class YandexDirect:
    def __init__(self, clientLogin, token):
        if sys.version_info < (3,):
            def u(x):
                try:
                    return x.encode("utf8")
                except UnicodeDecodeError:
                    return x
        else:
            def u(x):
                if type(x) == type(b''):
                    return x.decode('utf8')
                else:
                    return x
        self.__rURL = 'https://api.direct.yandex.com/json/v5/'
        self.__client_id = 'yandex_api_client_id'
        self.__clientLogin = clientLogin
        self.__token = token

    def __get_response(self, method, params):
        body = {"method": "get",  # Используемый метод.
                "params": params}
        headers = {"Authorization": "Bearer " + self.__token,  # OAuth-токен. Использование слова Bearer обязательно
                   "Client-Login": self.__clientLogin,  # Логин клиента рекламного агентства
                   "Accept-Language": "ru",  # Язык ответных сообщений
                   }
        jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
        response = requests.post(self.__rURL + method, jsonBody, headers=headers).json()
        return response

    def __get_campaign_settings(self, array, dicts):
        for key, value in array.items():
            if key == 'CounterIds':
                if value is not None:
                    dicts['CounterIds'] = ','.join([str(x) for x in value['Items']])
            elif isinstance(value, dict):
                self.__get_campaign_settings(value, dicts)
            else:
                if key == 'Amount':
                    dicts[key] = value / 1000000
                else:
                    dicts[key] = value
        return dicts

    def __slice(self, array, step, list_of_values):
        if len(array) > 0:
            list_of_values.append(array[:step])
            return self.__slice(array[step:], step, list_of_values)
        else:
            return list_of_values

    def get_campaigns(self):
        # TODO: Limit and Offset
        params = {"SelectionCriteria": {
        },
            "FieldNames": ["Id", "Name", "Status", "State", "Type", "Currency",
                           "StartDate", "EndDate", "TimeZone", "DailyBudget", "ClientInfo", "Statistics"],
            "TextCampaignFieldNames": ["CounterIds"],
            "DynamicTextCampaignFieldNames": ["CounterIds"],
            "CpmBannerCampaignFieldNames": ["CounterIds"]

        }
        all_campaigns = self.__get_response('campaigns', params)['result']['Campaigns']
        middle_list = []
        for element in all_campaigns:
            middle_dict = {}
            result = self.__get_campaign_settings(element, middle_dict)
            middle_list.append(middle_dict)
        campaign_ids = pd.DataFrame(middle_list)['Id'].tolist()
        middle_list = pd.DataFrame(middle_list).reset_index(drop=True)
        return middle_list, campaign_ids

    def get_groups(self, campaign_ids):
        start = True
        slice_list = []
        slice_list = self.__slice(campaign_ids, 10, slice_list)
        for element_in_list in slice_list:
            params = {"SelectionCriteria": {
                "CampaignIds": element_in_list
            },
                "FieldNames": ["CampaignId", "Id", "Name", "RegionIds", "ServingStatus", "Status",
                               "Subtype", "Type"]

            }

            all_groups = self.__get_response('adgroups', params)['result']['AdGroups']
            if start:
                df = pd.DataFrame(all_groups)
                start = False
            else:
                df = pd.concat([df, pd.DataFrame(all_groups)])
        all_groups_settings = df.reset_index(drop=True)
        group_ids = all_groups_settings['Id'].tolist()
        return all_groups_settings, group_ids

    def get_ads(self, group_ids):
        start = True
        slice_list = []
        slice_list = self.__slice(group_ids, 1000, slice_list)
        all_ads_setting = []
        for element_in_list in slice_list:
            params = {"SelectionCriteria": {
                "AdGroupIds": element_in_list
            },  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                "FieldNames": ["AdCategories", "AdGroupId", "CampaignId", "Id", "State", "Status",
                               "Type", "Subtype"],
                "TextAdFieldNames": ["Href", "Text", "Title", "Title2", "Mobile", "DisplayUrlPath"],
                "MobileAppAdFieldNames": ["Title", "Text", "TrackingUrl"],
                "TextImageAdFieldNames": ["Href"],
                "MobileAppImageAdFieldNames": ["TrackingUrl"],
                "TextAdBuilderAdFieldNames": ["Creative", "Href"],
                "MobileAppAdBuilderAdFieldNames": ["Creative", "TrackingUrl"],
                "CpcVideoAdBuilderAdFieldNames": ["Creative", "Href"],
                "CpmBannerAdBuilderAdFieldNames": ["Creative", "Href"]
            }

            all_ads_settings = self.__get_response('ads', params)['result']['Ads']
            for element in all_ads_settings:
                ads_settings = {}
                for ad_key, ad_value in element.items():
                    if type(ad_value) == dict:
                        for key, value in ad_value.items():
                            ads_settings[key] = value
                    else:
                        ads_settings[ad_key] = ad_value
                all_ads_setting.append(ads_settings)
        all_ads_settings = pd.DataFrame(all_ads_setting).reset_index(drop=True)
        return all_ads_settings

    def get_keywords(self, group_ids):
        start = True
        slice_list = []
        slice_list = self.__slice(group_ids, 100, slice_list)
        all_keyword_setting = []
        for element_in_list in slice_list:
            params = {"SelectionCriteria": {
                "AdGroupIds": element_in_list
            },  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                "FieldNames": ["Id", "Keyword", "State", "Status", "ServingStatus", "AdGroupId",
                               "CampaignId"]
            }

            all_keyword_settings = self.__get_response('keywords', params)['result']['Keywords']
            for element in all_keyword_settings:
                keyword_settings = {}
                for ad_key, ad_value in element.items():
                    if type(ad_value) == dict:
                        for key, value in ad_value.items():
                            keyword_settings[key] = value
                    else:
                        keyword_settings[ad_key] = ad_value
                all_keyword_setting.append(keyword_settings)
        all_keyword_settings = pd.DataFrame(all_keyword_setting).reset_index(drop=True)
        return all_keyword_settings

    def get_retargeting_list(self, group_ids):
        start = True
        slice_list = []
        slice_list = self.__slice(group_ids, 1000, slice_list)
        all_retargeting_list_setting = []
        for element_in_list in slice_list:
            params = {"SelectionCriteria": {
                "AdGroupIds": element_in_list
            },  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                "FieldNames": ["Id", "AdGroupId", "CampaignId", "RetargetingListId", "InterestId", "State"]
            }

            all_retargeting_list_settings = self.__get_response('audiencetargets', params)['result']['AudienceTargets']
            for element in all_retargeting_list_settings:
                retargeting_list_settings = {}
                for ad_key, ad_value in element.items():
                    if type(ad_value) == dict:
                        for key, value in ad_value.items():
                            retargeting_list_settings[key] = value
                    else:
                        retargeting_list_settings[ad_key] = ad_value
                all_retargeting_list_setting.append(retargeting_list_settings)
        all_retargeting_list_settings = pd.DataFrame(all_retargeting_list_setting).reset_index(drop=True)
        return all_retargeting_list_settings

    def get_retargeting_descript(self):
        params = {"SelectionCriteria": {
        },  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
            "FieldNames": ["Type", "Id", "Name", "Description", "IsAvailable", "Scope"]
        }

        all_retargeting_list_settings = self.__get_response('retargetinglists', params)['result']['RetargetingLists']
        return all_retargeting_list_settings

    def to_dataFrame(self, array):
        df = pd.DataFrame(array)
        return df
# TODO: Подключить проверку обновлений
# TODO: Подключить обработку ошибок
# TODO: Подключить автоматическое переключение на агентские баллы (если нужно и есть возможность)
# TODO: Объединить в одну функцию блок с делением ID и получением результата из нарезаного листа
# TODO: Добавить функцию получения результата с Offset, Limit и LimitedBy
# TODO: Добавить возможность получать список клиентов
# TODO: Получение справочных данных
# TODO: Добавить удаление дублей ключевых слов
# TODO: Получаем стату...
# TODO: Сводим стату с данными кабинета
# TODO: Заменяем спец параметры в UTM метках из кабинета
# TODO: Добавить подсчет API баллов
# TODO: Выделить выполнение запроса и формирование header и body в отдльный запрос и добавить обработку ошибок.
#  Переработать блок из старого кода

"""
try:
    body = self.__create_body(selection_criteria, field_names, offset)
    result = requests.post(self.__url + method, body, headers=headers)
    if result.status_code != 200 or result.json().get("error", False):
        logs = self.__get_headers(result, action)
        list_of_logs.append(logs)
        massage = f"Произошла ошибка при обращении к серверу API Директа.\nКод ошибки: {logs[5]}\nОписание ошибки: {logs[6]}"
        # Добавить self.__error_handling
        return list_of_logs, massage
    else:
        logs = self.__get_headers(result, action)
        list_of_logs.append(logs)
        for item in result.json()['result'][method.capitalize()]:
            total_result.append(item)
        if result.json()['result'].get('LimitedBy', False):
            offset = result.json()['result']['LimitedBy']
            self.__get_request(method, selection_criteria, field_names, headers, action, total_result, list_of_logs, offset)                                              
        return list_of_logs, total_result
except ConnectionError:
    massage = "Произошла ошибка соединения с сервером API."
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # TODO: В заголовке ответа указывается чьи баллы списались Units-Used-Login
    logs = ([7777777, timestamp, self.__client_login, 20, '<no data>', 'ConnectionError', 'Connection error', action])
    list_of_logs.append(logs)
    # Добавить self.__error_handling
    return list_of_logs, massage
"""