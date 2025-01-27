import aiohttp
import asyncio
import bs4
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import requests_cache

import aiohttp
import asyncio
import bs4
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import requests_cache

# Enable caching for all requests
requests_cache.install_cache('scrape_cache', expire_after=3600)  # Cache expires after 1 hour

# Global lists to store results
revistas_links = []
links_artigos = []
url = 'https://www.actamedicaportuguesa.com/revista/index.php/amp/issue/archive/'

# Function to fetch revista links asynchronously
async def fetch_revista_links(session, archive):
    archive_url = f'{url}{archive}'
    print(f'Fetching revista links from: {archive_url}')
    async with session.get(archive_url) as response:
        text = await response.text()
        bs = bs4.BeautifulSoup(text, "lxml")
        return [revista['href'] for revista in bs.find_all('a', class_='title')]

# Function to fetch artigo links asynchronously
async def fetch_artigo_links(session, revista_url):
    print(f'Fetching artigo links from: {revista_url}')
    async with session.get(revista_url) as response:
        text = await response.text()
        bs = bs4.BeautifulSoup(text, "lxml")
        data = bs.findAll('h3', attrs={'class': 'title'})
        return [a['href'] for div in data for a in div.findAll('a')]

# Function to fetch artigo data asynchronously
async def fetch_artigo_data(session, artigo_url):
    print(f'Fetching artigo data from: {artigo_url}')
    async with session.get(artigo_url) as response:
        text = await response.text()
        dom = bs4.BeautifulSoup(text, "lxml")
        
        revista = dom.select_one('nav.cmp_breadcrumbs li:nth-of-type(3) a').text.strip()
        ISSN = dom.find('meta', attrs={"name": "DC.Source.ISSN"}).get("content")
        volume = dom.find('meta', attrs={"name": "DC.Source.Volume"}).get("content", 'Não disponível')
        numero = dom.find('meta', attrs={"name": "DC.Source.Issue"}).get("content", 'Não disponivel')
        submetido = dom.find('meta', attrs={"name": "DC.Date.dateSubmitted"}).get("content", 'Não disponível')
        publicado = dom.find('meta', attrs={"name": "DC.Date.created"}).get("content", 'Não disponível')
        abstract = dom.find('meta', attrs={"name": "DC.Description"}).get("content", 'Não fornecido')
        titulo = dom.select_one('h1.page_title').text.strip()
        seccao = dom.select_one('nav.cmp_breadcrumbs li:nth-of-type(4) span').text.strip()
        citacao = dom.select_one('div.csl-entry').text.strip()
        DOI = dom.find('section', attrs={"class": "item doi"})
        DOI = next((a.get('href') for a in DOI.find_all('a')), 'Não fornecido') if DOI else 'Não fornecido'

        autores = []
        for name in dom.find_all(name='span', class_='name'):
            name_str = name.text.strip()
            affiliation = next((aff.text.strip() for aff in name.find_next_siblings(name='span') if aff.attrs.get('class', ())[0] == 'affiliation'), None)
            autores.append((name_str, affiliation))

        return [(revista, ISSN, volume, numero, submetido, publicado, titulo, seccao, DOI, autor[0], autor[1], citacao) for autor in autores]

# Main function to orchestrate multithreading and asynchronous tasks
async def main():
    async with aiohttp.ClientSession() as session:
        # Step 1: Fetch revista links using multithreading
        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, fetch_revista_links, session, archive) for archive in range(15)]
            revistas_links_results = await asyncio.gather(*tasks)
            # Change here: Ensure the results are retrieved from the Futures
            revistas_links = [link for sublist in [await result for result in revistas_links_results] for link in sublist]


        print(f'Número de revistas encontradas: {len(revistas_links)}')

        # Step 2: Fetch artigo links using multithreading
        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, fetch_artigo_links, session, revista_url) for revista_url in revistas_links]
            links_artigos_results = await asyncio.gather(*tasks)
            links_artigos = [link for sublist in links_artigos_results for link in sublist]  # Flatten list

        print(f'Número de artigos encontrados: {len(links_artigos)}')

        # Step 3: Fetch artigo data using multithreading
        all_data = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, fetch_artigo_data, session, artigo_url) for artigo_url in links_artigos]
            all_data_results = await asyncio.gather(*tasks)
            all_data = [item for sublist in all_data_results for item in sublist]  # Flatten list

        # Step 4: Save results to CSV and Excel
        df = pd.DataFrame(data=all_data, columns=['Revista', 'ISSN', 'Volume', 'Número', 'Submissao', 'Data de Publicação', 'Titulo', 'Secçao', 'DOI', 'Autor', 'Afiliação', 'Citação'])
        df.to_csv('acta_medica.csv', sep='|', encoding='utf-8', index=False)
        df.to_excel('acta_medica.xlsx', index=False)

# Entry point for the script
if __name__ == "__main__":
    # Check if running in a Jupyter notebook or similar environment
    try:
        import nest_asyncio
        nest_asyncio.apply()  # Enable nested event loops
    except ImportError:
        pass

    # Run the main function
    asyncio.run(main())
  
