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
    """Web scraper for RPMGF academic articles.
    
    This class handles the entire scraping process:
    1. Finding magazine/issue links from the archive
    2. Extracting article links from each magazine
    3. Scraping detailed data from each article
    4. Saving results to Excel and CSV files
    """
    
    def __init__(self, base_url: str = 'https://rpmgf.pt/ojs/index.php/rpmgf/issue/archive', 
                 max_workers: int = 5, delay: float = 1.0):
        self.base_url = base_url
        self.max_workers = max_workers  # Number of threads for concurrent processing
        self.delay = delay  # Seconds to wait between requests (rate limiting)
        self.revistas_links: List[str] = []  # Store magazine/issue URLs
        self.links_artigos: List[str] = []   # Store individual article URLs
        
        # Create a session for better performance and to maintain cookies/headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        print("🔧 RPMGF Scraper Initialized")
        print(f"   Base URL: {self.base_url}")
        print(f"   Concurrent Workers: {self.max_workers}")
        print(f"   Request Delay: {self.delay} seconds")
        print("   📝 Using session for better performance")
        
    def _make_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retries and error handling.
        
        This method handles:
        - Making HTTP requests with the session
        - Implementing retry logic for failed requests
        - Rate limiting to be respectful to the server
        - Error handling for network issues
        """
        print(f"   🌐 Requesting: {url}")
        
        for attempt in range(retries):
            try:
                # Wait between requests to avoid overwhelming the server
                print(f"   ⏳ Waiting {self.delay}s before request...")
                time.sleep(self.delay)
                
                # Make the HTTP request
                print(f"   📡 Attempt {attempt + 1}/{retries} - Fetching content...")
                response = self.session.get(url, timeout=10)
                response.raise_for_status()  # Raise exception for bad status codes
                
                print(f"   ✅ Success! Status: {response.status_code}, Size: {len(response.content)} bytes")
                return response
                
            except requests.RequestException as e:
                print(f"   ❌ Attempt {attempt + 1} failed: {str(e)[:100]}...")
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                
                if attempt == retries - 1:
                    print(f"   💥 All {retries} attempts failed for {url}")
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None
        return None
    
    def get_magazine_links(self) -> List[str]:
        """Extract magazine links from ALL archive pages (with pagination).
        
        This function:
        1. Fetches the main archive page
        2. Analyzes pagination to find total number of pages
        3. Visits all archive pages (not just the first one)
        4. Parses HTML on each page to find magazine/issue links
        5. Looks for <a> tags with class 'title'
        6. Converts relative URLs to absolute URLs
        7. Returns a list of ALL complete magazine URLs across all pages
        """
        print("\n🔍 STEP 1: Getting Magazine Links (All Pages)")
        print(f"   📄 Analyzing archive pagination...")
        
        # First, get the first page to understand pagination structure
        first_page_response = self._make_request(self.base_url)
        if not first_page_response:
            print("   ❌ Failed to get first archive page")
            return []
        
        try:
            print("   🧠 Parsing first page to detect pagination...")
            first_page_soup = bs4.BeautifulSoup(first_page_response.content, "lxml")
            
            # Detect pagination information
            pagination_info = self._detect_pagination(first_page_soup)
            if not pagination_info:
                print("   ⚠️  No pagination detected, scraping single page only")
                total_pages = 1
            else:
                total_pages = pagination_info['total_pages']
                issues_per_page = pagination_info['issues_per_page']
                total_issues = pagination_info['total_issues']
                
                print(f"   📊 Pagination detected:")
                print(f"      Total issues: {total_issues}")
                print(f"      Issues per page: {issues_per_page}")
                print(f"      Total pages: {total_pages}")
            
            # Extract magazine links from all pages
            all_magazine_links = []
            
            for page_num in range(1, total_pages + 1):
                print(f"\n   📄 Processing page {page_num}/{total_pages}")
                
                # Construct page URL
                if page_num == 1:
                    page_url = self.base_url
                else:
                    page_url = f"{self.base_url}/{page_num}"
                
                print(f"      URL: {page_url}")
                
                # Fetch the page
                page_response = self._make_request(page_url)
                if not page_response:
                    print(f"      ❌ Failed to get page {page_num}, skipping...")
                    continue
                
                # Parse the page
                page_soup = bs4.BeautifulSoup(page_response.content, "lxml")
                page_magazines = self._extract_magazine_links_from_page(page_soup, page_url)
                
                all_magazine_links.extend(page_magazines)
                print(f"      ✅ Found {len(page_magazines)} magazines on page {page_num}")
            
            # Store all links in instance variable for later use
            self.revistas_links = all_magazine_links
            
            print(f"\n   🎉 COMPLETED: Found {len(all_magazine_links)} total magazines across {total_pages} pages!")
            print("   💾 Stored in self.revistas_links for later use")
            
            logger.info(f"Found {len(all_magazine_links)} magazines across {total_pages} pages")
            return all_magazine_links
            
        except Exception as e:
            print(f"   💥 Error processing magazine links: {e}")
            logger.error(f"Error processing magazine links: {e}")
            return []

    def _detect_pagination(self, soup: bs4.BeautifulSoup) -> Optional[Dict]:
        """Detect pagination information from archive page.
        
        Looks for pagination text like "1-50 of 169" and "Next" links
        to determine the total number of pages and issues.
        
        Args:
            soup: BeautifulSoup parsed HTML of the archive page
            
        Returns:
            Dictionary with pagination info or None if not detected
        """
        try:
            # Look for pagination text (e.g., "1-50 of 169")
            pagination_text = soup.get_text()
            
            # Search for pattern like "1-50 of 169" or "1-25 of 355"
            import re
            pagination_match = re.search(r'(\d+)-(\d+)\s+of\s+(\d+)', pagination_text)
            
            if pagination_match:
                start_issue = int(pagination_match.group(1))
                end_issue = int(pagination_match.group(2))
                total_issues = int(pagination_match.group(3))
                
                issues_per_page = end_issue - start_issue + 1
                total_pages = (total_issues + issues_per_page - 1) // issues_per_page  # Ceiling division
                
                return {
                    'start_issue': start_issue,
                    'end_issue': end_issue,
                    'total_issues': total_issues,
                    'issues_per_page': issues_per_page,
                    'total_pages': total_pages
                }
            
            # Alternative: look for "Next" link to estimate pages
            next_link = soup.find('a', string=re.compile(r'próximo|next|Seguinte', re.I))
            if next_link:
                # If there's a next link, there are at least 2 pages
                # Estimate based on common patterns (usually 25-50 per page)
                print("      📄 Found 'Next' link, estimating multiple pages...")
                return {
                    'total_issues': 169,  # Known from analysis
                    'issues_per_page': 50,  # Known from analysis  
                    'total_pages': 4  # Known from analysis
                }
            
            return None
            
        except Exception as e:
            print(f"      ⚠️  Error detecting pagination: {e}")
            return None

    def _extract_magazine_links_from_page(self, soup: bs4.BeautifulSoup, page_url: str) -> List[str]:
        """Extract magazine links from a single archive page.
        
        Args:
            soup: BeautifulSoup parsed HTML of the archive page
            page_url: URL of the current page (for constructing absolute URLs)
            
        Returns:
            List of magazine URLs found on this page
        """
        try:
            magazine_links = []
            
            # Find all <a> tags with class 'title' (these are the magazine links)
            print(f"      🔍 Looking for <a> tags with class 'title'...")
            for revista in soup.find_all('a', class_='title'):
                href = revista.get('href')  # Get the URL from href attribute
                if href:
                    # Convert relative URL to absolute URL using the base URL
                    full_url = urljoin(self.base_url, href)
                    magazine_links.append(full_url)
                    print(f"         Found: {revista.get_text()[:30]}... -> {full_url}")
            
            return magazine_links
            
        except Exception as e:
            print(f"      💥 Error extracting magazine links from page: {e}")
            logger.error(f"Error extracting magazine links from {page_url}: {e}")
            return []
    
    def get_article_links(self) -> List[str]:
        """Extract article links from all magazines.
        
        This function:
        1. Gets the magazine links from the previous step
        2. Visits each magazine page
        3. Finds individual article links in <h3 class='title'> elements
        4. Converts relative article URLs to absolute URLs
        5. Returns a complete list of all article URLs
        """
        print("\n🔍 STEP 2: Getting Article Links from All Magazines")
        
        # Get magazine links from previous step
        revistas_links = self.get_magazine_links()
        if not revistas_links:
            print("   ❌ No magazines found to process")
            return []
            
        n_revistas = len(revistas_links)
        print(f"   📚 Found {n_revistas} magazines to process (this may take a while)")
        print("   🔄 Will visit each magazine to find articles")
        print("   ⏱️  Estimated time: ~" + str(n_revistas * 2 // 60) + "-" + str(n_revistas * 3 // 60) + " minutes")
        
        article_links = []
        processed_count = 0
        
        # Process each magazine one by one
        for index, url in enumerate(revistas_links, 1):
            processed_count += 1
            
            # Progress update every 10 magazines or at the end
            if processed_count % 10 == 0 or processed_count == n_revistas:
                print(f"\n   📊 Progress: {processed_count}/{n_revistas} magazines processed")
            
            print(f"\n   📖 Magazine {index}/{n_revistas}:")
            print(f"      URL: {url}")
            
            # Fetch the magazine page
            response = self._make_request(url)
            if not response:
                print(f"      ⚠️  Skipping magazine {index} (failed to load)")
                continue
                
            try:
                # Parse the magazine page to find articles
                print(f"      🧠 Parsing magazine page for articles...")
                soup = bs4.BeautifulSoup(response.content, "lxml")
                
                # Find article links in <h3> elements with class 'title'
                print(f"      🔍 Looking for <h3 class='title'> elements...")
                data = soup.findAll('h3', attrs={'class': 'title'})
                articles_in_magazine = 0
                
                for div in data:
                    for a in div.findAll('a'):  # Find all <a> links within the <h3>
                        href = a.get('href')
                        if href:
                            # Convert to absolute URL
                            full_url = urljoin(url, href)
                            article_links.append(full_url)
                            articles_in_magazine += 1
                            print(f"         Article found: {a.get_text()[:30]}...")
                
                print(f"      ✅ Found {articles_in_magazine} articles in this magazine")
                            
            except Exception as e:
                print(f"      💥 Error parsing articles from magazine: {e}")
                logger.error(f"Error parsing articles from {url}: {e}")
                continue
        
        # Store all article links
        self.links_artigos = article_links
        
        print(f"\n   📊 SUMMARY: Found {len(article_links)} total articles from {n_revistas} magazines")
        print("   💾 Stored in self.links_artigos for detailed scraping")
        
        logger.info(f"Found {len(article_links)} articles from {n_revistas} magazines")
        return article_links
    
    def extract_article_data(self, article_url: str) -> Optional[ArticleData]:
        """Extract detailed data from a single article.
        
        This function:
        1. Fetches the individual article page
        2. Extracts metadata from meta tags (ISSN, volume, dates, etc.)
        3. Extracts content from HTML elements (title, citation, etc.)
        4. Finds author names and their affiliations
        5. Creates ArticleData objects (one per author)
        6. Returns list of articles or None if failed
        """
        print(f"\n   📄 Article Details:")
        print(f"      URL: {article_url}")
        
        # Fetch the article page
        response = self._make_request(article_url)
        if not response:
            print(f"      ❌ Failed to load article page")
            return None
            
        try:
            print(f"      🧠 Parsing article content...")
            # Parse the article HTML
            soup = bs4.BeautifulSoup(response.content, "lxml")
            
            # Extract metadata from HTML meta tags (Dublin Core metadata)
            print(f"      📊 Extracting metadata from <meta> tags...")
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
            
            print(f"         Title: {titulo[:50] if titulo else 'Not found'}...")
            print(f"         Volume: {volume}, Issue: {numero}")
            print(f"         ISSN: {issn}")
            
            # Extract DOI (Digital Object Identifier)
            print(f"      🔗 Extracting DOI...")
            doi = self._extract_doi(soup)
            if doi:
                print(f"         DOI: {doi}")
            else:
                print(f"         DOI: Not found")
            
            # Extract authors and their affiliations
            print(f"      👥 Extracting authors and affiliations...")
            author_data = self._extract_authors_affiliations(soup)
            print(f"         Found {len(author_data)} author(s)")
            for i, (autor, afiliacao) in enumerate(author_data):
                print(f"         Author {i+1}: {autor} ({afiliacao})")
            
            # Create ArticleData object for each author
            # (Multiple authors = multiple records in our data)
            print(f"      📝 Creating ArticleData objects...")
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
            
            print(f"      ✅ Successfully extracted {len(articles_data)} record(s)")
            return articles_data if articles_data else None
            
        except Exception as e:
            print(f"      💥 Error extracting data: {e}")
            logger.error(f"Error extracting data from {article_url}: {e}")
            return None
    
    def _safe_extract(self, soup: bs4.BeautifulSoup, selector: str, attr: str = 'text') -> Optional[str]:
        """Safely extract text or attribute from BeautifulSoup element.
        
        This helper function:
        1. Uses CSS selector to find elements
        2. Safely extracts text content or attributes
        3. Returns None if element not found (instead of crashing)
        4. Strips whitespace from text content
        
        Args:
            soup: BeautifulSoup parsed HTML
            selector: CSS selector to find the element
            attr: Which attribute to extract ('text' for text content)
        
        Returns:
            The extracted text/attribute or None if not found
        """
        try:
            # Find element using CSS selector (e.g., 'h1.page_title')
            element = soup.select_one(selector)
            if element:
                if attr == 'text':
                    # Extract and clean text content
                    return element.get(attr, '').strip()
                else:
                    # Extract attribute (e.g., 'href', 'src')
                    return element.get(attr, '')
        except Exception as e:
            print(f"         ⚠️  Safe extract failed for {selector}: {e}")
            pass
        return None
    
    def _safe_extract_meta(self, soup: bs4.BeautifulSoup, name: str) -> Optional[str]:
        """Safely extract meta tag content.
        
        This helper function:
        1. Looks for <meta> tags with specific names
        2. Extracts the 'content' attribute safely
        3. Returns None if meta tag not found
        
        Commonly used meta tags:
        - DC.Source.ISSN: Journal ISSN
        - DC.Source.Volume: Volume number
        - DC.Source.Issue: Issue number
        - DC.Date.created: Publication date
        - DC.Date.dateSubmitted: Submission date
        
        Args:
            soup: BeautifulSoup parsed HTML
            name: The meta tag name (e.g., 'DC.Source.ISSN')
        
        Returns:
            The meta content or None if not found
        """
        try:
            # Find meta tag with the specified name attribute
            meta = soup.find('meta', attrs={"name": name})
            if meta:
                return meta.get("content")
            else:
                print(f"         ⚠️  Meta tag '{name}' not found")
                return None
        except Exception as e:
            print(f"         ⚠️  Meta extraction failed for {name}: {e}")
            return None
    
    def _extract_doi(self, soup: bs4.BeautifulSoup) -> Optional[str]:
        """Extract DOI from article page.
        
        DOI (Digital Object Identifier) is a unique identifier for academic papers.
        This function looks for a section with class 'item doi' and extracts
        the link inside it.
        
        Args:
            soup: BeautifulSoup parsed HTML
        
        Returns:
            DOI URL or None if not found
        """
        try:
            # Look for section with DOI information
            doi_section = soup.find('section', attrs={"class": "item doi"})
            if doi_section:
                # Find the link inside the DOI section
                link = doi_section.find('a')
                if link:
                    doi_url = link.get('href')
                    print(f"         📋 DOI found: {doi_url}")
                    return doi_url
                else:
                    print(f"         ⚠️  DOI section found but no link")
            else:
                print(f"         ⚠️  DOI section not found")
        except Exception as e:
            print(f"         ⚠️  DOI extraction failed: {e}")
            pass
        return None
    
    def _extract_authors_affiliations(self, soup: bs4.BeautifulSoup) -> List[Tuple[str, str]]:
        """Extract authors and their affiliations.
        
        This function:
        1. Finds all <span class='name'> elements (author names)
        2. For each name, finds the corresponding affiliation
        3. Returns list of (author_name, affiliation) tuples
        
        The affiliation is usually the next <span class='affiliation'> element
        after the author's name.
        
        Args:
            soup: BeautifulSoup parsed HTML
        
        Returns:
            List of (author_name, affiliation) tuples
        """
        print(f"         🔍 Looking for author information...")
        authors_data = []
        
        try:
            # Find all author name elements
            name_elements = soup.find_all(name='span', class_='name')
            print(f"         Found {len(name_elements)} author name elements")
            
            for i, name_element in enumerate(name_elements):
                # Get the author's name text
                name_text = name_element.text.strip()
                
                # Find the corresponding affiliation
                affiliation = self._find_affiliation_for_name(name_element)
                
                authors_data.append((name_text, affiliation))
                print(f"         Author {i+1}: '{name_text}' -> '{affiliation}'")
                
        except Exception as e:
            print(f"         ⚠️  Error extracting authors: {e}")
            logger.warning(f"Error extracting authors: {e}")
            
        return authors_data
    
    def _find_affiliation_for_name(self, name_element) -> str:
        """Find affiliation for a given name element.
        
        This function:
        1. Looks at the elements that come after the author's name
        2. Checks if any of them have class 'affiliation'
        3. If it finds the next author's name, stops searching
        4. Returns the affiliation text or 'Não disponível'
        
        The HTML structure typically looks like:
        <span class="name">Author Name</span>
        <span class="affiliation">Author's Institution</span>
        <span class="name">Next Author</span>
        
        Args:
            name_element: The BeautifulSoup element for the author's name
        
        Returns:
            The affiliation text or 'Não disponível'
        """
        try:
            # Look at elements that come after the name element (siblings)
            for sibling in name_element.find_next_siblings():
                if hasattr(sibling, 'attrs') and 'class' in sibling.attrs:
                    classes = sibling.attrs['class']
                    
                    if 'affiliation' in classes:
                        # Found the affiliation!
                        affiliation_text = sibling.text.strip()
                        print(f"            ✅ Found affiliation: {affiliation_text}")
                        return affiliation_text
                        
                    elif 'name' in classes:
                        # Found the next author, current author has no affiliation
                        print(f"            ❌ No affiliation found (next author starts)")
                        break
        except Exception as e:
            print(f"            ⚠️  Error finding affiliation: {e}")
            pass
        
        print(f"            ❌ No affiliation found")
        return "Não disponível"
    
    def scrape_all_articles(self) -> List[ArticleData]:
        """Scrape all articles using concurrent requests.
        
        This is the main function that:
        1. Gets all article URLs from the previous steps
        2. Uses ThreadPoolExecutor to process multiple articles simultaneously
        3. Each thread calls extract_article_data() for one article
        4. Collects all results into a single list
        5. Returns all article records found
        
        Benefits of concurrent processing:
        - Much faster than processing one by one
        - Better resource utilization
        - Can handle hundreds of articles efficiently
        """
        print("\n🔍 STEP 3: Scraping Detailed Article Data")
        
        # Get article URLs from the previous step
        article_links = self.get_article_links()
        if not article_links:
            print("   ❌ No articles found to scrape")
            return []
            
        n_articles = len(article_links)
        print(f"   📄 Found {n_articles} articles to process")
        print(f"   🧵 Using {self.max_workers} concurrent workers")
        print("   ⚡ Starting parallel processing...")
        
        logger.info(f"Starting to scrape {len(article_links)} articles")
        all_articles = []
        processed_count = 0
        success_count = 0
        
        # Create a thread pool for concurrent processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            print(f"   🎯 Submitting {n_articles} tasks to thread pool...")
            
            # Submit all article scraping tasks to the thread pool
            # Each task will call extract_article_data() for one URL
            future_to_url = {executor.submit(self.extract_article_data, url): url 
                           for url in article_links}
            
            print("   ⏳ Processing articles as they complete...")
            
            # Process results as they complete (instead of waiting for all)
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                processed_count += 1
                
                try:
                    # Get the result from the completed task
                    result = future.result()
                    if result:
                        if isinstance(result, list):
                            # Multiple authors = multiple records
                            all_articles.extend(result)
                            success_count += len(result)
                        else:
                            # Single record
                            all_articles.append(result)
                            success_count += 1
                        
                        # Progress update every 10 articles or at the end
                        if processed_count % 10 == 0 or processed_count == n_articles:
                            print(f"   📊 Progress: {processed_count}/{n_articles} articles processed, {success_count} records collected")
                    else:
                        print(f"   ⚠️  Article {processed_count} failed or returned no data")
                        
                except Exception as e:
                    print(f"   💥 Error processing article {processed_count}: {e}")
                    logger.error(f"Error processing {url}: {e}")
        
        print(f"\n   📈 FINAL RESULTS:")
        print(f"   Total articles processed: {processed_count}/{n_articles}")
        print(f"   Successfully scraped: {success_count} article records")
        print(f"   Success rate: {(success_count/len(article_links)*100):.1f}%")
        
        logger.info(f"Successfully scraped {len(all_articles)} article records")
        return all_articles
    
    def save_to_excel(self, articles: List[ArticleData], filename: str = 'artigos.xlsx'):
        """Save articles data to Excel file with formatting.
        
        This function:
        1. Converts ArticleData objects to a pandas DataFrame
        2. Saves to Excel with openpyxl engine
        3. Auto-adjusts column widths for better readability
        4. Adds proper sheet name and structure
        
        Args:
            articles: List of ArticleData objects to save
            filename: Output Excel filename
        """
        print(f"\n💾 STEP 4: Saving to Excel")
        
        if not articles:
            print("   ❌ No articles to save")
            logger.warning("No articles to save")
            return
            
        try:
            print(f"   📊 Converting {len(articles)} article records to DataFrame...")
            
            # Convert ArticleData objects to dictionaries for DataFrame
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
            
            # Create pandas DataFrame (like a table in memory)
            df = pd.DataFrame(data)
            print(f"   ✅ DataFrame created with {len(df)} rows and {len(df.columns)} columns")
            
            # Save to Excel with formatting
            print(f"   📝 Saving to Excel: {filename}")
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Write data to Excel sheet
                df.to_excel(writer, index=False, sheet_name='Artigos')
                
                # Get the workbook and worksheet objects for formatting
                workbook = writer.book
                worksheet = writer.sheets['Artigos']
                
                # Auto-adjust column widths based on content
                print(f"   📏 Adjusting column widths...")
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    # Find the longest content in each column
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    # Set column width (with reasonable limits)
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            print(f"   ✅ Successfully saved {len(articles)} articles to {filename}")
            print(f"   📋 Columns: {', '.join(df.columns)}")
            
            logger.info(f"Saved {len(articles)} articles to {filename}")
            
        except Exception as e:
            print(f"   💥 Error saving to Excel: {e}")
            logger.error(f"Error saving to Excel: {e}")
    
    def save_to_csv(self, articles: List[ArticleData], filename: str = 'artigos.csv'):
        """Save articles data to CSV file.
        
        This function:
        1. Converts ArticleData objects to a pandas DataFrame
        2. Saves to CSV with pipe (|) separator
        3. Uses UTF-8 encoding to support special characters
        4. No index column in the output
        
        Args:
            articles: List of ArticleData objects to save
            filename: Output CSV filename
        """
        print(f"\n💾 STEP 5: Saving to CSV")
        
        if not articles:
            print("   ❌ No articles to save")
            logger.warning("No articles to save")
            return
            
        try:
            print(f"   📊 Converting {len(articles)} article records to DataFrame...")
            
            # Convert ArticleData objects to dictionaries for DataFrame
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
            
            # Create pandas DataFrame
            df = pd.DataFrame(data)
            print(f"   ✅ DataFrame created with {len(df)} rows and {len(df.columns)} columns")
            
            # Save to CSV with pipe separator and UTF-8 encoding
            print(f"   📝 Saving to CSV: {filename}")
            print(f"   🔤 Using UTF-8 encoding and pipe (|) separator")
            
            df.to_csv(filename, sep='|', encoding='utf-8', index=False)
            
            print(f"   ✅ Successfully saved {len(articles)} articles to {filename}")
            print(f"   📋 Columns: {', '.join(df.columns)}")
            
            logger.info(f"Saved {len(articles)} articles to {filename}")
            
        except Exception as e:
            print(f"   💥 Error saving to CSV: {e}")
            logger.error(f"Error saving to CSV: {e}")

def main():
    """Main function to run the scraper.
    
    This is the entry point of the program. It:
    1. Sets up the scraper with configuration
    2. Runs the complete scraping process
    3. Saves results to files
    4. Provides a summary of results
    5. Handles errors gracefully
    """
    print("🚀 RPMGF Article Scraper - Starting Up")
    print("="*60)
    print("📚 Scraping academic articles from RPMGF journal")
    print("🔧 Using concurrent processing for better performance")
    print("💾 Output: Excel and CSV files with article data")
    print("="*60)
    
    logger.info("Starting RPMGF Article Scraper")
    
    # Initialize scraper with conservative settings for reliability
    print("\n⚙️  INITIALIZING SCRAPER")
    print("   🎯 Target: All 169 RPMGF magazine issues (across 4 archive pages)")
    print("   📊 Estimated scope: ~1000+ articles")
    print("   ⏱️  Estimated time: 30-60 minutes")
    print("   💡 This will comprehensively scrape the entire journal archive")
    
    scraper = RPMGFScraper(
        max_workers=3,  # Use 3 threads (be gentle to the server)
        delay=1.0      # Wait 1 second between requests
    )
    
    try:
        # Run the complete scraping process
        print("\n🎯 STARTING COMPREHENSIVE SCRAPING PROCESS")
        print("   This will:")
        print("   1. Find all 169 magazine issues across 4 archive pages")
        print("   2. Extract all article URLs from every magazine")
        print("   3. Scrape detailed metadata from each article")
        print("   4. Save comprehensive results to Excel and CSV")
        print("   📋 Expected output: Complete RPMGF journal database")
        
        articles = scraper.scrape_all_articles()
        
        if not articles:
            print("\n❌ NO ARTICLES FOUND")
            print("   This could mean:")
            print("   - The website structure has changed")
            print("   - Network connectivity issues")
            print("   - The archive page is empty")
            logger.warning("No articles found")
            return
        
        print(f"\n🎉 SUCCESS! Found {len(articles)} article records")
        
        # Create output directory
        print(f"\n📁 CREATING OUTPUT DIRECTORY")
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        print(f"   Created: {output_dir.absolute()}")
        
        # Save to both Excel and CSV formats
        print(f"\n💾 SAVING RESULTS")
        scraper.save_to_excel(articles, output_dir / 'artigos.xlsx')
        scraper.save_to_csv(articles, output_dir / 'artigos.csv')
        
        # Print comprehensive summary
        print(f"\n{'='*60}")
        print(f"🎉 SCRAPING COMPLETED SUCCESSFULLY! 🎉")
        print(f"{'='*60}")
        print(f"📊 RESULTS SUMMARY:")
        print(f"   • Total article records: {len(articles)}")
        print(f"   • Unique articles: {len(set(a.url for a in articles))}")
        print(f"   • Unique authors: {len(set(a.autor for a in articles))}")
        print(f"   • Processing method: Concurrent ({scraper.max_workers} workers)")
        
        print(f"\n📁 OUTPUT FILES:")
        print(f"   • Excel: {output_dir}/artigos.xlsx")
        print(f"   • CSV: {output_dir}/artigos.csv")
        print(f"   • Log: scraper.log")
        
        print(f"\n📋 DATA COLUMNS:")
        print(f"   Revista, ISSN, Volume, Número, Submissão, Data de Publicação,")
        print(f"   Título, Seção, DOI, Autor, Afiliação, Citação, URL")
        
        print(f"\n🔍 NEXT STEPS:")
        print(f"   1. Open Excel file for easy viewing")
        print(f"   2. Use CSV for data analysis in Python/R")
        print(f"   3. Check log file for any warnings")
        
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print("\n⏹️  SCRAPING INTERRUPTED BY USER")
        print("   Use Ctrl+C to stop the process anytime")
        logger.info("Scraping interrupted by user")
        
    except Exception as e:
        print(f"\n💥 UNEXPECTED ERROR OCCURRED")
        print(f"   Error: {e}")
        print(f"   Check scraper.log for detailed error information")
        logger.error(f"Unexpected error: {e}")
        
    finally:
        print(f"\n🏁 SCRAPING FINISHED")
        logger.info("Scraping finished")

if __name__ == "__main__":
    main()
