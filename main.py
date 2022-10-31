import  aiohttp
import asyncio
from bs4 import BeautifulSoup
from postgresql import Database
import time
import json

db = Database()

async def main():
    start_time = time.time()
    with open('urls.txt', 'r') as urls:
        tasks = []
        for url in urls:
            tasks.append(asyncio.create_task(get_website_code(url.strip())))

    pages = await asyncio.gather(*tasks)
    for page in pages:
        if page[0]:
            data = get_data(page[0], page[1])
            db.add_data(data, page[1])

    finish_time = time.time() - start_time
    print(f'time for script ---- {finish_time}')


async def get_website_code(url):
    async with aiohttp.ClientSession() as session:
        response = await session.get(url, ssl=False)
        print(response.status)
        if response.status in [301, 302, 403, 404, 500]:
            print(f'-----------------------{response.status}-----------------------')
            return None

        return [await response.text(), url]

def get_data(page, url):
    soup = BeautifulSoup(page, 'html.parser')

    meta_discription = soup.find('meta', {'name': 'description'})
    if meta_discription:
        meta_discription = meta_discription.get('content')

    headers = {}
    for index, header in enumerate(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        headers[header] = [h1.text.strip() for h1 in soup.find_all(header)]

    schema_markup = [i.text.strip('\n') for i in soup.find_all('script', {'type': 'application/ld+json'})]

    with open('selectors.json', 'r') as file:
        selectors = json.load(file)
        elements = {}
        if url in selectors:
            for key, value in selectors[f'{url}'].items():
                elements[key] = soup.select(f'{value}')[0].text.strip()
        else:
            for key, value in selectors['for_all'].items():
                try:
                    elements[key] = soup.select(f'{value}')[0].text.strip()
                except IndexError:
                    elements[key] = None

    print(elements)


    data = {
        'title': soup.find('title').text.strip(),
        'meta_description': meta_discription,
        'headers': json.dumps(headers),
        'schema_markup': json.dumps(schema_markup),
        'text': soup.text.replace('\n', ''),
        'selectors': json.dumps(elements)
    }
    return data


if __name__ == '__main__':
    asyncio.run(main())