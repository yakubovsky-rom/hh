import requests
from yaml import load
class CallTouch:
    def __init__(self):
        self.__config = load(open('path to your yaml file', 'r'))
        self.__token_calltouch = self.__config['calltouch']['oauth']['token']
        self.__site_id = self.__config['calltouch']['oauth']['site_id']
        self.__calltouch_url = f"http://api.calltouch.ru/calls-service/RestAPI/{self.__site_id}/calls-diary/calls"
    def __get_responce(self, params):
        responce = requests.get(self.__calltouch_url, params = params)
        responce = responce.json()
        return responce
    def shema_for_bgq(self):
        schema = {'attribution': 'INTEGER',
                  'callClientUniqueId': 'STRING',
                  'callId': 'INTEGER',
                  'callReferenceId': 'STRING',
                  'callUrl': 'STRING',
                  'callbackCall': 'BOOLEAN',
                  'callerNumber': 'STRING',
                  'callphase': 'STRING',
                  'city': 'STRING',
                  'clientId': 'STRING',
                  'ctCallerId': 'STRING',
                  'date': 'STRING',
                  'duration': 'INTEGER',
                  'hostname': 'STRING',
                  'keyword': 'STRING',
                  'medium': 'STRING',
                  'order': 'STRING',
                  'phoneNumber': 'STRING',
                  'redirectNumber': 'STRING',
                  'sessionId': 'INTEGER',
                  'source': 'STRING',
                  'successful': 'BOOLEAN',
                  'targetCall': 'BOOLEAN',
                  'uniqTargetCall': 'BOOLEAN',
                  'uniqueCall': 'BOOLEAN',
                  'url': 'STRING',
                  'userAgent': 'STRING',
                  'utmCampaign': 'STRING',
                  'utmContent': 'STRING',
                  'utmMedium': 'STRING',
                  'utmSource': 'STRING',
                  'utmTerm': 'STRING',
                  'waitingConnect': 'INTEGER',
                  'yaClientId': 'STRING'}
        return schema
    def get_calls(self, date_from, date_to, attribution = 1):
        #TODO: Тут добавить проверку формата даты и написать конвертер
        params = {'clientApiId': self.__token_calltouch, 'dateFrom': date_from,
                  'dateTo': date_to, 'page': 1, 'limit': 500}
        responce = self.__get_responce(params)
        count_of_pages = responce['pageTotal']
        if count_of_pages > 1:
            total_result = []
            for i in range(1, count_of_pages+1):
                params = {'clientApiId': self.__token_calltouch, 'dateFrom': date_from, 'dateTo': date_to,
                          'page': i, 'limit': 500}
                result = self.__get_responce(params)
                for item in result['records']:
                    total_result.append(item)
            return total_result
        total_result = [item for item in responce['records']]
        return total_result
    def get_calls_one_day(self, day, attribution = 1):
        pass
    def to_tuples_for_insert_rows(self, total_result):
        list_of_tuples = []
        for item in total_result:
            list_of_tuples.append(tuple([item['attribution'], item['callClientUniqueId'], item['callId'],
                                         item['callReferenceId'], item['callUrl'], item['callbackCall'],
                                         item['callerNumber'], item['callphase'], item['city'], item['clientId'],
                                         item['ctCallerId'], item['date'], item['duration'], item['hostname'],
                                         item['keyword'], item['medium'], item['order'], item['phoneNumber'],
                                         item['redirectNumber'], item['sessionId'], item['source'], item['successful'],
                                         item['targetCall'], item['uniqTargetCall'], item['uniqueCall'], item['url'],
                                         item['userAgent'], item['utmCampaign'], item['utmContent'], item['utmMedium'],
                                         item['utmSource'], item['utmTerm'],
                                         item['waitingConnect'], item['yaClientId']]))
        return list_of_tuples