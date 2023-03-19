import aiohttp
import asyncio
from bs4 import BeautifulSoup
from postgresql import Database
from async_postgresql import DataBaseClass
import time
import json
from urllib.parse import urlparse
from fake_useragent import UserAgent
from threading import Thread


async def create_gather():
    start_time = time.time()


    # urls from urls.txt
    with open('../urls.txt', 'r') as file:
        urls = file.readlines()

    # create session
    limit_connecting = 500
    connector = aiohttp.TCPConnector(limit=limit_connecting, ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=connector)

    # selectors from selectors.json
    with open('../selectors.json', 'r') as file:
        selectors = json.load(file)

    # create a db instance
    db = DataBaseClass

    # create and call a function gather
    tasks = []
    for url in urls:
        tasks.append(asyncio.create_task(main(url.strip(), session, selectors, db)))
    await asyncio.gather(*tasks)



    await session.close()
    print(f"\nОбщее затраченное время: {time.time() - start_time}")

async def main(url, session, selectors, db):
    page = await get_website_code(url, session)

    if page[0]:
        data = get_data(page[0], page[1], selectors)
        await db.add_data(data, page[1])

async def get_website_code(url, session):
    page = None
    try:
        headers = {
            'User-Agent': UserAgent().chrome
        }
        response = await session.post(url, headers=headers, ssl=False)

        if response.status != 200:
            print(f'-----------------------{response.status}----------------------- {url}')
        else:
            page = await response.text()
    except UnicodeDecodeError:
        print(f"------ {url} -------- UnicodeDecodeError")
    except TimeoutError:
        print(f"------ {url} -------- TimeoutError")
    except Exception as ex:
        print(f'------ {url} --------\n{ex}')
    finally:
        return [page, url]

def get_data(page, url, selectors):
    soup = BeautifulSoup(page, 'html.parser')

    title = soup.find('title')
    if title:
        title = title.text.strip()

    meta_description = soup.find('meta', {'name': 'description'})
    if meta_description:
        meta_description = meta_description.get('content')

    headers = {}
    for index, header in enumerate(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        headers[header] = [h1.text.strip() for h1 in soup.find_all(header)]

    schema_markup = [i.text.strip('\n') for i in soup.find_all('script', {'type': 'application/ld+json'})]

    elements = {}
    type_key = 'for_all'
    if url in selectors:
        type_key = url

    for key, value in selectors[type_key].items():
        try:
            if isinstance(value, list):
                elements[key] = soup.select(f'{value[0]}')[0].get(f'{value[1]}')
            else:
                elements[key] = soup.select(f'{value}')[0].text.strip()
        except IndexError:
            elements[key] = None


    data = {
        'title': title,
        'meta_description': meta_description,
        'headers': json.dumps(headers),
        'schema_markup': json.dumps(schema_markup),
        'text': soup.text.replace('\n', ''),
        'selectors': json.dumps(elements)
    }
    return data

# def thread(urls):
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(create_gather(urls))
#     loop.close()
#
# def create_threads():
#     amount_threads = int(input("How much do you wanna create threads?\n"))
#
#     start_time = time.time()
#
#     with open('../urls.txt', 'r') as file:
#         urls = file.readlines()
#         amount_urls = len(urls)
#         count = int(amount_urls/amount_threads)
#         print(count, amount_urls)
#         for item in range(count, amount_urls, count):
#             print(item)
#             Thread(target=thread(urls[item-count:item])).start()
#
#
#
#     finish_time = time.time() - start_time
#     print(f'time for script ---- {finish_time}s')



if __name__ == '__main__':
    asyncio.run(create_gather())