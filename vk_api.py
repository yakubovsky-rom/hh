import requests, json, time, pprint
import pandas as pd
import numpy as np

access_token = "access token"

class vk:
    __client_id = 'client id'
    __client_secret = "client secret"
    __redirect_uri = 'http://localhost:8888'
    __display = 'popup'
    __v = '5.101'
    __scopes = 'photos,video,wall,ads,offline,groups,stats,email'
    __method_url = "https://api.vk.com/method/"
    def __init__(self, user_id, access_token):
        self.user_id = user_id
        self.access_token = access_token
    def auth(self):
        code_url = f"https://oauth.vk.com/authorize?client_id={self.__client_id}&redirect_uri={self.__redirect_uri}&display={self.__display}&response_type=code&v={self.__v}&scope={self.__scopes}"
        print(f"Чтобы получить code, перейдите по ссылке: {code_url}")
        code = input('Введите полученный код: ')
        params = {'client_id': self.__client_id, 'code': code, 'client_secret': self.__client_secret, 'redirect_uri': self.__redirect_uri, 'v': self.__v}
        token = requests.get("https://oauth.vk.com/access_token", params = params).json()['access_token']
        name, surname = self.__getProfileInfo(token)
        return token, user_id
    def __getProfileInfo(self):
        params = {'access_token': self.access_token, 'v': self.__v, 'user_ids': self.user_id}
        ProfileInfo = requests.get(self.__method_url + 'users.get', params = params).json()['response'][0]
        return ProfileInfo['first_name'], ProfileInfo['last_name']
    def getAccounts(self):
        params = {'access_token': self.access_token, 'v': self.__v}
        Accounts = requests.get(self.__method_url + 'ads.getAccounts', params = params).json()['response']
        Accounts = pd.DataFrame(Accounts)
        name, surname = self.__getProfileInfo()
        Accounts['user_id'] = self.user_id
        Accounts['user'] = name + " " + surname
        return Accounts
    def getClients(self, Accounts):
        all_accounts = []
        for account in Accounts:
            params = {'access_token': self.access_token, 'v': self.__v, "account_id": account}
            result = requests.get(self.__method_url + 'ads.getClients', params = params).json()
            for element in result['response']:
                element['account_id'] = account
                element['user_id'] = self.user_id
                all_accounts.append(element)
            time.sleep(1)
            df = pd.DataFrame(all_accounts)
            df = df.rename(columns={'id': 'client_id', 'all_limit': 'client_all_limit', 'day_limit': 'client_day_limit', 'name': 'client_name'})
        return df
    def getCampaigns(self, account_id, client_id = None):
        if client_id:
            params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id, "include_deleted": 1, "client_id": client_id}
            Campaigns = requests.get(self.__method_url + 'ads.getCampaigns', params = params).json()['response']
            Campaigns = pd.DataFrame(Campaigns)
            Campaigns['client_id'] = client_id
        else:
            params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id, "include_deleted": 1}
            Campaigns = requests.get(self.__method_url + 'ads.getCampaigns', params = params).json()['response']
            Campaigns = pd.DataFrame(Campaigns)
        Campaigns = Campaigns.rename(columns={'all_limit': 'campaign_all_limit', 'create_time': 'campaign_create_time', 
                                            'day_limit': 'campaign_day_limit', 'id': 'campaign_id', 'name': 'campaign_name', 
                                            'start_time': 'campaign_start_time', 'status': 'campaign_status', 'stop_time': 'campaign_stop_time', 
                                            'type': 'campaign_type', 'update_time': 'campaign_update_time'})
        return Campaigns
    def getTargetGroups(self, account_id, client_id = None):
        if client_id:
            params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id, "client_id": client_id}
            Campaigns = requests.get(self.__method_url + 'ads.getTargetGroups', params = params).json()['response']
            Campaigns = pd.DataFrame(Campaigns)
            Campaigns['client_id'] = client_id
        else:
            params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id}
            Campaigns = requests.get(self.__method_url + 'ads.getTargetGroups', params = params).json()['response']
            Campaigns = pd.DataFrame(Campaigns)

        del Campaigns['target_pixel_rules']
        Campaigns['account_id'] = account_id
        return Campaigns
    def getAds(self, account_id, client_id = None):
        if client_id:
            params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id, "client_id": client_id}
            Ads = requests.get(self.__method_url + 'ads.getAds', params = params).json()['response']
            Ads = pd.DataFrame(Ads)
            Ads['client_id'] = client_id
        else:
            params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id}
            Ads = requests.get(self.__method_url + 'ads.getAds', params = params).json()['response']
            Ads = pd.DataFrame(Ads)
        del Ads['weekly_schedule_hours']
        del Ads['events_retargeting_groups']
        Ads['account_id'] = account_id
        return Ads
    def getDayAdsDemographics(self, account_id, ids, date_from, date_to):
        params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id, "ids_type": "ad", "ids": ids, 
                 "period": "day", "date_from": date_from, "date_to": date_to}
        Demographics = requests.get(self.__method_url + 'ads.getDemographics', params = params).json()['response']
        list_of_age = []
        list_of_sex = []
        list_of_sex_age = []
        for ad_stat in Demographics:
            ad_id = ad_stat['id']
            ad_type = ad_stat['type']
            ad_stats = ad_stat['stats']
            for one in ad_stats:
                ad_day = one['day']
                df_sex_age = pd.DataFrame(one['sex_age'])
                df_age = pd.DataFrame(one['age'])
                df_sex = pd.DataFrame(one['sex'])
                df_sex_age['ad_id'] = ad_id
                df_age['ad_id'] = ad_id
                df_sex['ad_id'] = ad_id
                df_sex_age['ad_type'] = ad_type
                df_age['ad_type'] = ad_type
                df_sex['ad_type'] = ad_type
                df_sex_age['day'] = ad_day
                df_age['day'] = ad_day
                df_sex['day'] = ad_day
                list_of_age.append(df_age)
                list_of_sex.append(df_sex)
                list_of_sex_age.append(df_sex_age)
        list_of_sex_age = pd.concat(list_of_sex_age).reset_index(drop=True).fillna(0)
        list_of_sex = pd.concat(list_of_sex).reset_index(drop=True).fillna(0)
        list_of_age = pd.concat(list_of_age).reset_index(drop=True).fillna(0)
        return list_of_age, list_of_sex, list_of_sex_age
    def getDayСampaignDemographics(self, account_id, ids, date_from, date_to):
        params = {'access_token': self.access_token, 'v': self.__v, "account_id": account_id, "ids_type": "campaign", "ids": ids, 
                 "period": "day", "date_from": date_from, "date_to": date_to}
        Demographics = requests.get(self.__method_url + 'ads.getDemographics', params = params).json()['response']
        list_of_age = []
        list_of_sex = []
        list_of_sex_age = []
        for campaign_stat in Demographics:
            campaign_id = campaign_stat['id']
            campaign_type = campaign_stat['type']
            campaign_stats = campaign_stat['stats']
            for one in campaign_stats:
                campaign_day = one['day']
                df_sex_age = pd.DataFrame(one['sex_age'])
                df_age = pd.DataFrame(one['age'])
                df_sex = pd.DataFrame(one['sex'])
                df_sex_age['campaign_id'] = campaign_id
                df_age['campaign_id'] = campaign_id
                df_sex['campaign_id'] = campaign_id
                df_sex_age['campaign_type'] = campaign_type
                df_age['campaign_type'] = campaign_type
                df_sex['campaign_type'] = campaign_type
                df_sex_age['day'] = campaign_day
                df_age['day'] = campaign_day
                df_sex['day'] = campaign_day
                list_of_age.append(df_age)
                list_of_sex.append(df_sex)
                list_of_sex_age.append(df_sex_age)
        list_of_sex_age = pd.concat(list_of_sex_age).reset_index(drop=True).fillna(0)
        list_of_sex = pd.concat(list_of_sex).reset_index(drop=True).fillna(0)
        list_of_age = pd.concat(list_of_age).reset_index(drop=True).fillna(0)
        return list_of_age, list_of_sex, list_of_sex_age



if __name__ = "__main__":
	main = vk("user id", access_token)

	Accounts = main.getAccounts()
	Accounts_id = Accounts[Accounts['account_type'] == 'agency']['account_id'].tolist()
	clients = main.getClients(Accounts_id)
	clients = clients.merge(Accounts)
	campaigns = main.getCampaigns("account id", "client id")
	campaigns = campaigns.merge(clients, left_on = 'client_id', right_on = 'client_id')
	ads = main.getAds("account id", "client id")

	campaign_id = ads['campaign_id'].tolist()
	campaign_id = [str(x) for x in campaign_id]
	campaign_id = ",".join(campaign_id)
	ads_id = ",".join(ads['id'].tolist())


	age, sex, sex_age = main.getDayAdsDemographics("account id", ads_id, "2019-10-13", "2019-10-15")
	age, sex, sex_age = main.getDayСampaignDemographics("account id", campaign_id, "2019-10-13", "2019-10-15")
