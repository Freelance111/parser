from config import config
import asyncpg
import asyncio

class DataBaseClass:
    def __init__(self):
        loop = asyncio.get_event_loop()
        self.connection = loop.run_until_complete(self.create_connection())

    async def create_connection(self):
        try:
            connection = await asyncpg.connect(**config)
            print('\tSuccessful connection')
            return connection
        except Exception as ex:
            print(f'\n\tConnection refused\n {ex}')


    async def add_data(self, data, url):
        sql = "INSERT INTO information(site, title, meta_description, headers, " \
              "selectors, schema_markup, text, task_time) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"

        async with self.connection.cursor() as cursor:
            await cursor.execute(sql, [url,
                                 data['title'],
                                 data['meta_description'],
                                 data['headers'],
                                 data['selectors'],
                                 data['schema_markup'],
                                 data['text']])

            await self.connection.commit()

    def __del__(self):
        self.connection.close()