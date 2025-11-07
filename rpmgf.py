"""
Academic Article Scraper for RPMGF Journal
Improved version with better error handling, concurrent requests, and cleaner code.
"""

import bs4
import requests
import pandas as pd
from typing import Iterator, List, Dict, Optional, Tuple
from itertools import islice
import openpyxl
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from urllib.parse import urljoin, urlparse
import re
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ArticleData:
    """Data class to store article information."""
    revista: str
    issn: str
    volume: str
    numero: str
    submissao: str
    publicado: str
    titulo: str
    seccao: str
    doi: str
    autor: str
    afiliacao: str
    citacao: str
    url: str

class RPMGFScraper:
    """Web scraper for RPMGF academic articles."""
    
    def __init__(self, base_url: str = 'https://rpmgf.pt/ojs/index.php/rpmgf/issue/archive', 
                 max_workers: int = 5, delay: float = 1.0):
        self.base_url = base_url
        self.max_workers = max_workers
        self.delay = delay
        self.revistas_links: List[str] = []
        self.links_artigos: List[str] = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def _make_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retries and error handling."""
        for attempt in range(retries):
            try:
                time.sleep(self.delay)  # Rate limiting
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == retries - 1:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None
        return None
    
    def get_magazine_links(self) -> List[str]:
        """Extract magazine links from the main archive page."""
        logger.info("Fetching magazine links...")
        
        response = self._make_request(self.base_url)
        if not response:
            return []
            
        try:
            soup = bs4.BeautifulSoup(response.content, "lxml")
            revista_links = []
            
            # Find magazine links with class 'title'
            for revista in soup.find_all('a', class_='title'):
                href = revista.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    revista_links.append(full_url)
            
            self.revistas_links = revista_links
            logger.info(f"Found {len(revista_links)} magazines")
            return revista_links
            
        except Exception as e:
            logger.error(f"Error parsing magazine links: {e}")
            return []
    
    def get_article_links(self) -> List[str]:
        """Extract article links from all magazines."""
        revistas_links = self.get_magazine_links()
        if not revistas_links:
            return []
            
        n_revistas = len(revistas_links)
        logger.info(f"Processing {n_revistas} magazines for article links")
        article_links = []
        
        for index, url in enumerate(revistas_links, 1):
            logger.info(f"Getting articles from magazine {index}/{n_revistas}: {url}")
            
            response = self._make_request(url)
            if not response:
                continue
                
            try:
                soup = bs4.BeautifulSoup(response.content, "lxml")
                
                # Find article links in h3 elements with class 'title'
                data = soup.findAll('h3', attrs={'class': 'title'})
                for div in data:
                    for a in div.findAll('a'):
                        href = a.get('href')
                        if href:
                            full_url = urljoin(url, href)
                            article_links.append(full_url)
                            
            except Exception as e:
                logger.error(f"Error parsing articles from {url}: {e}")
                continue
        
        self.links_artigos = article_links
        logger.info(f"Found {len(article_links)} articles total")
        return article_links
    
    def extract_article_data(self, article_url: str) -> Optional[ArticleData]:
        """Extract detailed data from a single article."""
        response = self._make_request(article_url)
        if not response:
            return None
            
        try:
            soup = bs4.BeautifulSoup(response.content, "lxml")
            
            # Extract basic metadata with error handling
            revista = self._safe_extract(soup, 'nav.cmp_breadcrumbs li:nth-of-type(3) a', 'text')
            issn = self._safe_extract_meta(soup, "DC.Source.ISSN")
            volume = self._safe_extract_meta(soup, "DC.Source.Volume")
            numero = self._safe_extract_meta(soup, "DC.Source.Issue") or "Não disponível"
            submissao = self._safe_extract_meta(soup, "DC.Date.dateSubmitted") or "Não disponível"
            publicado = self._safe_extract_meta(soup, "DC.Date.created")
            abstract = self._safe_extract_meta(soup, "DC.Description") or "Não fornecido"
            titulo = self._safe_extract(soup, 'h1.page_title', 'text')
            seccao = self._safe_extract(soup, 'nav.cmp_breadcrumbs li:nth-of-type(4) span', 'text')
            citacao = self._safe_extract(soup, 'div.csl-entry', 'text')
            
            # Extract DOI
            doi = self._extract_doi(soup)
            
            # Extract authors and affiliations
            author_data = self._extract_authors_affiliations(soup)
            
            # Create ArticleData object for each author
            articles_data = []
            for autor, afiliacao in author_data:
                article = ArticleData(
                    revista=revista or "Não identificado",
                    issn=issn or "Não disponível",
                    volume=volume or "Não disponível",
                    numero=numero,
                    submissao=submissao,
                    publicado=publicado or "Não disponível",
                    titulo=titulo or "Sem título",
                    seccao=seccao or "Não disponível",
                    doi=doi or "Não fornecido",
                    autor=autor or "Não disponível",
                    afiliacao=afiliacao or "Não disponível",
                    citacao=citacao or "Não disponível",
                    url=article_url
                )
                articles_data.append(article)
            
            return articles_data if articles_data else None
            
        except Exception as e:
            logger.error(f"Error extracting data from {article_url}: {e}")
            return None
    
    def _safe_extract(self, soup: bs4.BeautifulSoup, selector: str, attr: str = 'text') -> Optional[str]:
        """Safely extract text or attribute from BeautifulSoup element."""
        try:
            element = soup.select_one(selector)
            if element:
                return element.get(attr, '').strip() if attr == 'text' else element.get(attr, '')
        except Exception:
            pass
        return None
    
    def _safe_extract_meta(self, soup: bs4.BeautifulSoup, name: str) -> Optional[str]:
        """Safely extract meta tag content."""
        try:
            meta = soup.find('meta', attrs={"name": name})
            return meta.get("content") if meta else None
        except Exception:
            return None
    
    def _extract_doi(self, soup: bs4.BeautifulSoup) -> Optional[str]:
        """Extract DOI from article page."""
        try:
            doi_section = soup.find('section', attrs={"class": "item doi"})
            if doi_section:
                link = doi_section.find('a')
                if link:
                    return link.get('href')
        except Exception:
            pass
        return None
    
    def _extract_authors_affiliations(self, soup: bs4.BeautifulSoup) -> List[Tuple[str, str]]:
        """Extract authors and their affiliations."""
        authors_data = []
        
        try:
            name_elements = soup.find_all(name='span', class_='name')
            
            for name_element in name_elements:
                name_text = name_element.text.strip()
                affiliation = self._find_affiliation_for_name(name_element)
                authors_data.append((name_text, affiliation))
                
        except Exception as e:
            logger.warning(f"Error extracting authors: {e}")
            
        return authors_data
    
    def _find_affiliation_for_name(self, name_element) -> str:
        """Find affiliation for a given name element."""
        try:
            # Look for following siblings
            for sibling in name_element.find_next_siblings():
                if hasattr(sibling, 'attrs') and 'class' in sibling.attrs:
                    if 'affiliation' in sibling.attrs['class']:
                        return sibling.text.strip()
                    elif 'name' in sibling.attrs['class']:
                        # Found next author, no affiliation for current
                        break
        except Exception:
            pass
        return "Não disponível"
    
    def scrape_all_articles(self) -> List[ArticleData]:
        """Scrape all articles using concurrent requests."""
        article_links = self.get_article_links()
        if not article_links:
            return []
            
        logger.info(f"Starting to scrape {len(article_links)} articles")
        all_articles = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all article scraping tasks
            future_to_url = {executor.submit(self.extract_article_data, url): url 
                           for url in article_links}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if result:
                        if isinstance(result, list):
                            all_articles.extend(result)
                        else:
                            all_articles.append(result)
                except Exception as e:
                    logger.error(f"Error processing {url}: {e}")
        
        logger.info(f"Successfully scraped {len(all_articles)} article records")
        return all_articles
    
    def save_to_excel(self, articles: List[ArticleData], filename: str = 'artigos.xlsx'):
        """Save articles data to Excel file."""
        if not articles:
            logger.warning("No articles to save")
            return
            
        try:
            # Convert to DataFrame
            data = []
            for article in articles:
                data.append({
                    'Revista': article.revista,
                    'ISSN': article.issn,
                    'Volume': article.volume,
                    'Número': article.numero,
                    'Submissão': article.submissao,
                    'Data de Publicação': article.publicado,
                    'Título': article.titulo,
                    'Seção': article.seccao,
                    'DOI': article.doi,
                    'Autor': article.autor,
                    'Afiliação': article.afiliacao,
                    'Citação': article.citacao,
                    'URL': article.url
                })
            
            df = pd.DataFrame(data)
            
            # Save to Excel with formatting
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Artigos')
                
                # Get the workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['Artigos']
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Saved {len(articles)} articles to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")
    
    def save_to_csv(self, articles: List[ArticleData], filename: str = 'artigos.csv'):
        """Save articles data to CSV file."""
        if not articles:
            logger.warning("No articles to save")
            return
            
        try:
            data = []
            for article in articles:
                data.append({
                    'Revista': article.revista,
                    'ISSN': article.issn,
                    'Volume': article.volume,
                    'Número': article.numero,
                    'Submissão': article.submissao,
                    'Data de Publicação': article.publicado,
                    'Título': article.titulo,
                    'Seção': article.seccao,
                    'DOI': article.doi,
                    'Autor': article.autor,
                    'Afiliação': article.afiliacao,
                    'Citação': article.citacao,
                    'URL': article.url
                })
            
            df = pd.DataFrame(data)
            df.to_csv(filename, sep='|', encoding='utf-8', index=False)
            logger.info(f"Saved {len(articles)} articles to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

def main():
    """Main function to run the scraper."""
    logger.info("Starting RPMGF Article Scraper")
    
    # Initialize scraper
    scraper = RPMGFScraper(max_workers=3, delay=1.0)
    
    try:
        # Scrape all articles
        articles = scraper.scrape_all_articles()
        
        if not articles:
            logger.warning("No articles found")
            return
        
        # Create output directory
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        # Save to files
        scraper.save_to_excel(articles, output_dir / 'artigos.xlsx')
        scraper.save_to_csv(articles, output_dir / 'artigos.csv')
        
        # Print summary
        print(f"\n{'='*50}")
        print(f"SCRAPING COMPLETED SUCCESSFULLY")
        print(f"{'='*50}")
        print(f"Total articles processed: {len(articles)}")
        print(f"Output files:")
        print(f"  - Excel: {output_dir}/artigos.xlsx")
        print(f"  - CSV: {output_dir}/artigos.csv")
        print(f"  - Log: scraper.log")
        print(f"{'='*50}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Scraping finished")

if __name__ == "__main__":
    main()
