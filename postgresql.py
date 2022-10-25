from config import config
import psycopg2
import time


class Database:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config)
            print('\tSuccessful connection')
        except Exception as ex:
            print(f'\tConnection refused\n\n {ex}')

    async def add_data(self, data, url):
        sql = "UPDATE information SET site=%s, title=%s, meta_description=%s," \
              "headers=%s, schema_markup=%s, text=%s, task_time=NOW()"

        with self.connection.cursor() as cursor:
            cursor.execute(sql, [url,
                                 data['title'],
                                 data['meta_description'],
                                 data['headers'],
                                 data['schema_markup'],
                                 data['text']])

            self.connection.commit()