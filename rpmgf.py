import bs4
import requests
import pandas as pd
from typing import Iterator
from itertools import islice
import openpyxl

# def revistas = obtém os links de cada revista individual
def revistas():
  f = requests.get(url)
  bs = bs4.BeautifulSoup(f.content,"lxml")
  for revista in bs.find_all('a', class_= 'title'):
    revistas_links.append (revista['href'])
  return revistas_links

# def artigos = obtém os links de cada artigo individual
def artigos():
  revistas_links=revistas()
  n_revistas=len(revistas_links)
  print ('Número de revistas encontradas:',n_revistas)
  for index, url in enumerate(revistas_links,1):
    print('A obter artigos da revista n:',index,'de',n_revistas, 'em', url)
    revista=requests.get(url)
    texto = bs4.BeautifulSoup(revista.content,"lxml")
    data = texto.findAll('h3',attrs={'class':'title'})
    for div in data:
      for a in div.findAll('a'):
        links_artigos.append(a['href'])
  return links_artigos


def dados_artigos(self): #(links_artigos): #-> Iterator[tuple[str, str | None]]:
    links_artigos = artigos()
    n_artigos = len(links_artigos)
    print('Número de artigos encontrados:',n_artigos)
    for index, url in enumerate(links_artigos,1):
    #for artigo in links_artigos:
      print('A obter dados do artigo n:',index,'de',n_artigos,'em',url)
      artigo_web=requests.get(url)
      dom = bs4.BeautifulSoup(markup=artigo_web.text, features='lxml')
      revista = dom.select_one('nav.cmp_breadcrumbs li:nth-of-type(3) a').text.strip()
      ISSN = dom.find('meta', attrs={"name": "DC.Source.ISSN"}).get("content")
      volume = dom.find('meta', attrs={"name": "DC.Source.Volume"}).get("content")
      numero = dom.find('meta', attrs={"name": "DC.Source.Issue"}).get("content") if dom.find('meta', attrs={"name": "DC.Source.Issue"}) else 'Não disponivel'
      try:
        submetido = dom.find('meta', attrs={"name": "DC.Date.dateSubmitted"}).get("content")
      except:
         submetido='Não disponível'
      publicado = dom.find('meta', attrs={"name": "DC.Date.created"}).get("content")
      try:
        abstract = dom.find('meta', attrs={"name": "DC.Description"}).get("content")
      except:
        abstract='Não fornecido'
      titulo = dom.select_one('h1.page_title').text.strip()
      seccao = dom.select_one('nav.cmp_breadcrumbs li:nth-of-type(4) span').text.strip()
      citacao = dom.select_one('div.csl-entry').text.strip()
      try:
        DOI = dom.find('section', attrs={"class": "item doi"})
        for a in DOI.find_all('a'):
          DOI=(a.get('href'))
      except:
        DOI='Não fornecido'
      for name in dom.find_all(name='span', class_='name'):
        # Search through siblings for a matching affiliation tag
        for affiliation in name.find_next_siblings(name='span'):
            name_str = name.text.strip()
            class_ = affiliation.attrs.get('class', ())[0]
            if class_ == 'affiliation':
                # If we've found an affiliation class on the soonest span sibling, use it
                yield revista, ISSN, volume, numero, submetido, publicado, titulo, seccao, DOI, name_str, affiliation.text.strip(), citacao
                break
            elif class_ == 'name':
                # If we've encountered the next name, there is no affiliation.
                yield revista, ISSN, volume, numero, submetido, publicado, titulo, seccao, DOI, name_str, None, citacao
                break
        else:
            # If there are no span siblings, there is no affiliation.
            yield revista, ISSN, volume, numero, submetido, publicado, titulo, seccao, DOI, name_str, None, citacao

revistas_links=[]
links_artigos=[]
url = 'https://rpmgf.pt/ojs/index.php/rpmgf/issue/archive'

df = pd.DataFrame(data=dados_artigos(artigos), columns=['Revista', 'ISSN', 'Volume', 'Número', 'Submissao', 'Data de Publicação', 'Titulo', 'Secçao', 'DOI', 'Autor', 'Afiliação', 'Citação'])
df.to_csv('file_name.csv', sep='|', encoding='utf-8')
df.to_excel('artigos.xlsx')