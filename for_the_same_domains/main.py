import aiohttp
import asyncio
from bs4 import BeautifulSoup
from postgresql import Database
import time
import json
from urllib.parse import urlparse
from fake_useragent import UserAgent



db = Database()
async def main():
    start_time = time.time()
    with open('../urls.txt', 'r') as urls:
        tasks = []
        for url in urls:
            tasks.append(asyncio.create_task(get_website_code(url.strip())))

    pages = await asyncio.gather(*tasks)
    start_time_2 = time.time()
    time_for_db = 0
    for page in pages:
        if page[0]:
            data = get_data(page[0], page[1])
            start_time_3 = time.time()
            db.add_data(data, page[1])
            time_for_db += time.time()-start_time_3

    print(f"Время затраченное на запросы к дб: {time_for_db}")
    print(f"Время затраченное на синхронную часть: {time.time() - start_time_2}")


    finish_time = time.time() - start_time
    print(f'time for script ---- {finish_time}s')


list_urls = []
async def get_website_code(url):
    domain = '.'.join(urlparse(url.strip()).netloc.split('.')[:-1])
    while list_urls.count(domain) > 5:
        await asyncio.sleep(1)

    list_urls.append(domain)
    page = None
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                'User-Agent': UserAgent().chrome
            }
            response = await session.post(url, headers=headers)

        list_urls.remove(domain)
        if response.status in [301, 302, 403, 404, 500, 504]:
            print(f'-----------------------{response.status}----------------------- {url}')
        else:
            page = await response.text()
    except UnicodeDecodeError:
        print(f"------ {url} -------- Can't decode page")
    except Exception as ex:
        print(f'------ {url} --------\n{ex}')
    finally:
        return [page, url]

def get_data(page, url):
    soup = BeautifulSoup(page, 'html.parser')
    print(page)

    title = soup.find('title')
    if title:
        title = title.text.strip()

    meta_discription = soup.find('meta', {'name': 'description'})
    if meta_discription:
        meta_discription = meta_discription.get('content')

    headers = {}
    for index, header in enumerate(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        headers[header] = [h1.text.strip() for h1 in soup.find_all(header)]

    schema_markup = [i.text.strip('\n') for i in soup.find_all('script', {'type': 'application/ld+json'})]

    with open('../selectors.json', 'r') as file:
        selectors = json.load(file)
        elements = {}
        type_key = 'for_all'
        if url in selectors:
            type_key = url

        try:
            for key, value in selectors[type_key].items():
                if isinstance(value, list):
                    elements[key] = soup.select(f'{value[0]}')[0].get(f'{value[1]}')
                else:
                    elements[key] = soup.select(f'{value}')[0].text.strip()
        except IndexError:
            elements[key] = None


    data = {
        'title': title,
        'meta_description': meta_discription,
        'headers': json.dumps(headers),
        'schema_markup': json.dumps(schema_markup),
        'text': soup.text.replace('\n', ''),
        'selectors': json.dumps(elements)
    }
    return data


if __name__ == '__main__':
    asyncio.run(main())
