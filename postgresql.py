from config import config
import psycopg2


class Database:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config)
            print('\tSuccessful connection')
        except Exception as ex:
            print(f'\n\tConnection refused\n {ex}')

    def add_data(self, data, url):
        sql = "INSERT INTO information(site, title, meta_description, headers, " \
              "selectors, schema_markup, text, task_time) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"

        with self.connection.cursor() as cursor:
            cursor.execute(sql, [url,
                                 data['title'],
                                 data['meta_description'],
                                 data['headers'],
                                 data['selectors'],
                                 data['schema_markup'],
                                 data['text']])

            self.connection.commit()

