from google.cloud import bigquery
class BigQuery:
    def __init__(self, path_to_your_json_file):
        self.client = bigquery.Client.from_service_account_json(path_to_your_json_file)
        self.project = self.client.project
        self.datasets = [dataset.dataset_id for dataset in list(self.client.list_datasets())]
        self.tables_in_dataset = self.__get_tables()
    def __get_tables(self):
        tables = {}
        for dataset_id in self.datasets:
            dataset_ref = self.client.dataset(dataset_id)
            tables_list = list(self.client.list_tables(dataset_ref))
            tables[dataset_id] = [table.table_id for table in tables_list]
        return tables
    def __create_schema(self, schema_dict):
        schema = []
        for key, value in schema_dict.items():
            schema.append(bigquery.SchemaField(key, value))
        return schema
    def __create_list_of_values(self, array):
        array_list = []
        for list_values in array:
            result = []
            for item in list_values.values():
                result.append(item)
            array_list.append(tuple(result))
        return array_list
    def create_dataset(self, dataset_id):
        dataset_ref = self.client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = self.client.create_dataset(dataset)
        print(f'Dataset \"{dataset_id}\" создан.')
    def delete_dataset(self, dataset_id, delete_contents = False):
        dataset_ref = self.client.dataset(dataset_id)
        self.client.delete_dataset(dataset_ref, delete_contents)
        if delete_contents:
            print(f"Dataset \"{dataset_id}\" удален вместе с контентом.")
        else:
            print(f"Dataset \"{dataset_id}\" удален.")
    def create_table(self, dataset_id, table_id, schema_dict):
        dataset_ref = self.client.dataset(dataset_id)
        schema = self.__create_schema(schema_dict)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema = schema)
        table = self.client.create_table(table)
        print(f"Таблица \"{table_id}\" создана в \"{dataset_id}\".")
    def insert_rows(self, dataset_id, table_id, list_of_tuples):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        errors = self.client.insert_rows(table, list_of_tuples)
        return errors
    def get_table_shema(self, dataset_id, table_id):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)
        return table.schema
    def get_table_num_rows(self, dataset_id, table_id):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)
        return table.num_rows
    def get_query(self, sql):
        query_job = self.client.query(sql, location='US')
        result = []
        for row in query_job:
            result.append(list(row))
        return result