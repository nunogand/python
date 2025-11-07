#!/usr/bin/env python3
"""
Acta Médica Portuguesa Scraper
=============================

A comprehensive web scraper for collecting academic articles from Acta Médica Portuguesa journal.
This script extracts article data from the journal's archive and individual article pages.

Features:
- Concurrent processing for improved performance
- Comprehensive error handling and retry mechanisms
- Professional logging system
- Configurable settings
- Structured data output (Excel and CSV)
- Rate limiting to respect server resources

Author: MiniMax Agent
Date: November 2025
"""

import requests
import pandas as pd
import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import json


@dataclass
class AMPArticleData:
    """Structured data class for Acta Médica Portuguesa articles."""
    revista: str = ""
    issn: str = ""
    volume: str = ""
    numero: str = ""
    ano: str = ""
    periodo: str = ""
    titulo: str = ""
    autores: str = ""
    afiliacoes: str = ""
    doi: str = ""
    resumo: str = ""
    palavras_chave: str = ""
    secao: str = ""
    paginas: str = ""
    data_publicacao: str = ""
    url_artigo: str = ""
    url_pdf: str = ""
    licenca: str = ""
    formatos_citacao: str = ""


class AMPScraper:
    """
    Acta Médica Portuguesa Scraper
    
    A professional web scraper for collecting article metadata from Acta Médica Portuguesa journal.
    """
    
    def __init__(self, base_url: str = "https://www.actamedicaportuguesa.com/revista/index.php/amp/", 
                 max_workers: int = 5, delay: float = 1.0, retry_attempts: int = 3):
        """
        Initialize the Acta Médica Portuguesa scraper.
        
        Args:
            base_url: Base URL for the journal
            max_workers: Number of concurrent threads for processing
            delay: Delay between requests in seconds
            retry_attempts: Number of retry attempts for failed requests
        """
        self.base_url = base_url
        self.archive_url = urljoin(base_url, "issue/archive/")
        self.max_workers = max_workers
        self.delay = delay
        self.retry_attempts = retry_attempts
        
        # Initialize session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Setup logging
        self._setup_logging()
        
        print(f"🔧 Initializing Acta Médica Portuguesa scraper...")
        print(f"   Base URL: {self.base_url}")
        print(f"   Archive URL: {self.archive_url}")
        print(f"   Max workers: {self.max_workers}")
        print(f"   Request delay: {self.delay}s")
        print(f"   Retry attempts: {self.retry_attempts}")
        print(f"✅ Scraping initialized successfully")
    
    def _setup_logging(self):
        """Setup comprehensive logging system."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('acta_medica_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("🚀 Acta Médica Portuguesa scraper starting...")
    
    def _make_request(self, url: str, retry_count: int = 3) -> Optional[requests.Response]:
        """
        Make HTTP request with retry mechanism and error handling.
        
        Args:
            url: URL to request
            retry_count: Current retry attempt number
            
        Returns:
            Response object or None if failed
        """
        print(f"🌐 Making HTTP request to: {url}")
        
        for attempt in range(retry_count, self.retry_attempts + 1):
            try:
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    print(f"✅ Request successful (Status: {response.status_code})")
                    return response
                elif response.status_code == 404:
                    print(f"❌ Resource not found (404): {url}")
                    return None
                else:
                    print(f"⚠️  HTTP {response.status_code} error on attempt {self.retry_attempts - attempt + 1}: {url}")
                    if attempt < self.retry_attempts:
                        wait_time = (self.retry_attempts - attempt) * 2
                        print(f"⏳ Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        
            except requests.exceptions.RequestException as e:
                print(f"🚫 Request failed (attempt {self.retry_attempts - attempt + 1}): {e}")
                if attempt < self.retry_attempts:
                    wait_time = (self.retry_attempts - attempt) * 2
                    print(f"⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
        
        print(f"❌ All {self.retry_attempts} attempts failed for: {url}")
        return None
    
    def get_archive_pages(self) -> List[str]:
        """
        Get all archive page URLs.
        
        Returns:
            List of archive page URLs
        """
        print("📋 Getting archive pages...")
        
        # First, get the total number of pages by checking page 1
        response = self._make_request(self.archive_url)
        if not response:
            print("❌ Failed to get first archive page")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find pagination info (e.g., "1-25 of 355")
        pagination_text = soup.get_text()
        page_count = 1
        
        # Extract total issue count from pagination
        if 'of' in pagination_text:
            try:
                parts = pagination_text.split('of')
                if len(parts) > 1:
                    total_issues = int(parts[1].strip().split()[0])
                    issues_per_page = 25  # Based on the structure observed
                    page_count = (total_issues + issues_per_page - 1) // issues_per_page
            except (ValueError, IndexError):
                # If parsing fails, use a reasonable default
                page_count = 15  # 355 issues / 25 per page ≈ 15 pages
        
        print(f"📊 Found approximately {page_count} archive pages ({total_issues if 'total_issues' in locals() else 'estimated'} total issues)")
        
        # Generate all archive page URLs
        archive_pages = []
        for page_num in range(1, page_count + 1):
            if page_num == 1:
                archive_pages.append(self.archive_url)
            else:
                archive_pages.append(f"{self.archive_url}{page_num}")
        
        print(f"✅ Generated {len(archive_pages)} archive page URLs")
        return archive_pages
    
    def extract_issue_links(self, archive_page_url: str) -> List[str]:
        """
        Extract issue links from an archive page.
        
        Args:
            archive_page_url: URL of the archive page
            
        Returns:
            List of issue page URLs
        """
        print(f"🔍 Extracting issue links from: {archive_page_url}")
        
        response = self._make_request(archive_page_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        issue_links = []
        
        # Look for issue links (typically in the format /issue/view/[volume])
        issue_links_found = soup.find_all('a', href=re.compile(r'/issue/view/\d+'))
        
        for link in issue_links_found:
            full_url = urljoin(self.base_url, link.get('href'))
            if full_url not in issue_links:
                issue_links.append(full_url)
        
        print(f"✅ Found {len(issue_links)} issue links on this page")
        return issue_links
    
    def get_all_issue_links(self) -> List[str]:
        """
        Get all issue links from all archive pages.
        
        Returns:
            List of all issue page URLs
        """
        print("📚 Extracting all issue links from archive...")
        
        archive_pages = self.get_archive_pages()
        all_issue_links = []
        
        for i, archive_page in enumerate(archive_pages, 1):
            print(f"\n📄 Processing archive page {i}/{len(archive_pages)}")
            issue_links = self.extract_issue_links(archive_page)
            all_issue_links.extend(issue_links)
            
            # Add delay between pages
            if i < len(archive_pages):
                time.sleep(self.delay)
        
        # Remove duplicates
        all_issue_links = list(set(all_issue_links))
        print(f"📊 Total unique issues found: {len(all_issue_links)}")
        
        return sorted(all_issue_links)
    
    def extract_article_links_from_issue(self, issue_url: str) -> List[Dict[str, str]]:
        """
        Extract article links and basic info from an issue page.
        
        Args:
            issue_url: URL of the issue page
            
        Returns:
            List of dictionaries with article info
        """
        print(f"📰 Extracting articles from issue: {issue_url}")
        
        response = self._make_request(issue_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles_info = []
        
        # Extract issue metadata
        issue_title = soup.find('h1')
        issue_info = {
            'issue_url': issue_url,
            'issue_title': issue_title.get_text().strip() if issue_title else "",
            'volume': "",
            'number': "",
            'year': "",
            'period': ""
        }
        
        # Parse volume, number, and year from title (e.g., "Vol. 38 No. 11 (2025)")
        if issue_info['issue_title']:
            title_match = re.search(r'Vol\.\s*(\d+)\s*No\.\s*(\d+)\s*\((\d+)\)', issue_info['issue_title'])
            if title_match:
                issue_info['volume'] = title_match.group(1)
                issue_info['number'] = title_match.group(2)
                issue_info['year'] = title_match.group(3)
        
        # Find article links (typically H3 headings with links)
        article_titles = soup.find_all(['h1', 'h2', 'h3', 'h4'], text=re.compile(r'.+'))
        
        for title_elem in article_titles:
            article_link = title_elem.find('a')
            if article_link and article_link.get('href'):
                article_url = urljoin(self.base_url, article_link.get('href'))
                
                # Get authors (usually in the same container or nearby)
                authors_text = ""
                # Look for author information in the same container or following elements
                parent = title_elem.parent
                if parent:
                    # Try to find author text in the same container
                    author_elem = parent.find(text=re.compile(r'[A-Z][a-z]+.*[A-Z][a-z]+'))
                    if author_elem:
                        authors_text = author_elem.strip()
                
                # Get page numbers
                page_text = ""
                if parent:
                    # Look for page numbers in the container
                    page_elem = parent.find(text=re.compile(r'\d+-\d+'))
                    if page_elem:
                        page_text = page_elem.strip()
                
                article_info = {
                    'article_url': article_url,
                    'title': title_elem.get_text().strip(),
                    'authors': authors_text,
                    'pages': page_text,
                    'issue_info': issue_info.copy()
                }
                
                articles_info.append(article_info)
        
        print(f"✅ Found {len(articles_info)} articles in this issue")
        
        # Print sample article info for verification
        if articles_info:
            print(f"   Sample article: {articles_info[0]['title'][:50]}...")
        
        return articles_info
    
    def extract_article_data(self, article_info: Dict[str, str]) -> AMPArticleData:
        """
        Extract detailed metadata from an individual article page.
        
        Args:
            article_info: Basic article information from issue page
            
        Returns:
            AMPArticleData object with full metadata
        """
        print(f"📖 Extracting detailed data for: {article_info['title'][:50]}...")
        
        response = self._make_request(article_info['article_url'])
        if not response:
            # Return basic info if detailed extraction fails
            return AMPArticleData(
                titulo=article_info['title'],
                autores=article_info['authors'],
                volume=article_info['issue_info']['volume'],
                numero=article_info['issue_info']['number'],
                ano=article_info['issue_info']['year'],
                periodo=article_info['issue_info']['period'],
                paginas=article_info['pages'],
                url_artigo=article_info['article_url']
            )
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Initialize article data
        article_data = AMPArticleData()
        article_data.revista = "Acta Médica Portuguesa"
        article_data.issn = "2182-5173"
        article_data.volume = article_info['issue_info']['volume']
        article_data.numero = article_info['issue_info']['number']
        article_data.ano = article_info['issue_info']['year']
        article_data.periodo = article_info['issue_info']['period']
        article_data.titulo = article_info['title']
        article_data.autores = article_info['authors']
        article_data.paginas = article_info['pages']
        article_data.url_artigo = article_info['article_url']
        
        # Extract detailed metadata
        try:
            # DOI
            doi_elem = soup.find(text=re.compile(r'doi', re.IGNORECASE))
            if doi_elem:
                doi_text = doi_elem.parent.get_text() if doi_elem.parent else ""
                doi_match = re.search(r'10\.\d+/[^\s]+', doi_text)
                if doi_match:
                    article_data.doi = doi_match.group()
            
            # Abstract
            abstract_elem = soup.find(['div', 'p'], class_=re.compile(r'abstract', re.IGNORECASE))
            if not abstract_elem:
                abstract_elem = soup.find(text=re.compile(r'Abstract', re.IGNORECASE))
                if abstract_elem:
                    abstract_elem = abstract_elem.parent
            
            if abstract_elem:
                article_data.resumo = abstract_elem.get_text().strip()
            
            # Keywords
            keywords_elem = soup.find(text=re.compile(r'Keywords?', re.IGNORECASE))
            if keywords_elem:
                keywords_text = keywords_elem.parent.get_text()
                keywords_match = re.search(r'Keywords?:\s*(.+)', keywords_text, re.IGNORECASE)
                if keywords_match:
                    article_data.palavras_chave = keywords_match.group(1).strip()
            
            # Article section
            section_elem = soup.find(['span', 'div'], class_=re.compile(r'section|category', re.IGNORECASE))
            if section_elem:
                article_data.secao = section_elem.get_text().strip()
            
            # Publication date
            date_elem = soup.find(text=re.compile(r'Published|Date', re.IGNORECASE))
            if date_elem:
                date_text = date_elem.parent.get_text()
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', date_text)
                if date_match:
                    article_data.data_publicacao = date_match.group(1)
            
            # PDF link
            pdf_link = soup.find('a', href=re.compile(r'/article/view/.*/.*'))
            if pdf_link:
                article_data.url_pdf = urljoin(self.base_url, pdf_link.get('href'))
            
            # License information
            license_elem = soup.find(text=re.compile(r'license|License', re.IGNORECASE))
            if license_elem:
                license_text = license_elem.parent.get_text()
                article_data.licenca = license_text.strip()
            
            # Citation formats
            citation_elem = soup.find(text=re.compile(r'Citation|Cite', re.IGNORECASE))
            if citation_elem:
                citation_text = citation_text = citation_elem.parent.get_text()
                formats = re.findall(r'\b(ACM|ACS|APA|Chicago|Harvard|IEEE)\b', citation_text)
                if formats:
                    article_data.formatos_citacao = ', '.join(formats)
            
            print(f"✅ Successfully extracted detailed metadata")
            
        except Exception as e:
            print(f"⚠️  Error extracting some detailed metadata: {e}")
        
        return article_data
    
    def scrape_all_articles(self, max_issues: Optional[int] = None) -> List[AMPArticleData]:
        """
        Scrape all articles from the journal.
        
        Args:
            max_issues: Maximum number of issues to process (for testing)
            
        Returns:
            List of AMPArticleData objects
        """
        print("🚀 Starting comprehensive article scraping...")
        print("=" * 60)
        
        # Get all issue links
        issue_links = self.get_all_issue_links()
        
        if max_issues:
            issue_links = issue_links[:max_issues]
            print(f"🔢 Limited to first {max_issues} issues for testing")
        
        print(f"📊 Processing {len(issue_links)} issues...")
        
        all_articles_data = []
        
        # Process issues concurrently
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit article extraction tasks
            future_to_issue = {
                executor.submit(self.extract_article_links_from_issue, issue_url): issue_url 
                for issue_url in issue_links
            }
            
            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_issue), 1):
                issue_url = future_to_issue[future]
                
                try:
                    articles_info = future.result()
                    print(f"\n📋 Processed issue {i}/{len(issue_links)}: {len(articles_info)} articles found")
                    
                    # Extract detailed data for each article
                    for article_info in articles_info:
                        try:
                            article_data = self.extract_article_data(article_info)
                            all_articles_data.append(article_data)
                            
                            # Rate limiting
                            time.sleep(self.delay)
                            
                        except Exception as e:
                            print(f"🚫 Error processing article {article_info['title']}: {e}")
                            continue
                    
                except Exception as e:
                    print(f"🚫 Error processing issue {issue_url}: {e}")
                    continue
        
        print(f"\n🎉 Scraping completed!")
        print(f"📊 Total articles collected: {len(all_articles_data)}")
        print(f"📈 Issues processed: {len(issue_links)}")
        print(f"⚡ Average articles per issue: {len(all_articles_data)/len(issue_links):.1f}")
        
        return all_articles_data
    
    def save_results(self, articles_data: List[AMPArticleData], 
                    excel_file: str = 'acta_medica_portuguesa_artigos.xlsx',
                    csv_file: str = 'acta_medica_portuguesa_artigos.csv') -> None:
        """
        Save scraped data to Excel and CSV files.
        
        Args:
            articles_data: List of article data objects
            excel_file: Output Excel filename
            csv_file: Output CSV filename
        """
        print(f"💾 Saving results to files...")
        print(f"   Excel file: {excel_file}")
        print(f"   CSV file: {csv_file}")
        
        if not articles_data:
            print("❌ No data to save")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame([article.__dict__ for article in articles_data])
        
        # Reorder columns for better readability
        column_order = [
            'revista', 'issn', 'volume', 'numero', 'ano', 'periodo',
            'titulo', 'autores', 'afiliacoes', 'doi', 'resumo', 
            'palavras_chave', 'secao', 'paginas', 'data_publicacao',
            'url_artigo', 'url_pdf', 'licenca', 'formatos_citacao'
        ]
        
        # Only include columns that exist
        existing_columns = [col for col in column_order if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in column_order]
        df = df[existing_columns + remaining_columns]
        
        # Save to Excel with formatting
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Artigos')
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Artigos']
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
            
            print(f"✅ Excel file saved successfully ({len(articles_data)} articles)")
            
        except Exception as e:
            print(f"❌ Error saving Excel file: {e}")
        
        # Save to CSV
        try:
            df.to_csv(csv_file, index=False, encoding='utf-8', sep='|')
            print(f"✅ CSV file saved successfully ({len(articles_data)} articles)")
            
        except Exception as e:
            print(f"❌ Error saving CSV file: {e}")
        
        # Print summary statistics
        print(f"\n📊 Data Summary:")
        print(f"   📰 Total articles: {len(articles_data)}")
        print(f"   📅 Date range: {df['ano'].min() if not df['ano'].empty else 'N/A'} - {df['ano'].max() if not df['ano'].empty else 'N/A'}")
        print(f"   📚 Total volumes: {df['volume'].nunique() if not df['volume'].empty else 'N/A'}")
        print(f"   🔢 Total issues: {len(df['volume'].astype(str) + '-' + df['numero'].astype(str)) if not df.empty else 'N/A'}")
        
        if not df['doi'].empty:
            print(f"   🔗 Articles with DOI: {df['doi'].notna().sum()}/{len(df)} ({df['doi'].notna().sum()/len(df)*100:.1f}%)")
        
        if not df['autores'].empty:
            avg_authors = df['autores'].str.split(',').str.len().mean()
            print(f"   👥 Average authors per article: {avg_authors:.1f}")
    
    def get_statistics(self, articles_data: List[AMPArticleData]) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the scraped data.
        
        Args:
            articles_data: List of article data objects
            
        Returns:
            Dictionary with statistics
        """
        if not articles_data:
            return {}
        
        df = pd.DataFrame([article.__dict__ for article in articles_data])
        
        stats = {
            'total_articles': len(articles_data),
            'date_range': {
                'earliest': df['ano'].min() if not df['ano'].empty else None,
                'latest': df['ano'].max() if not df['ano'].empty else None
            },
            'volumes': {
                'count': df['volume'].nunique() if not df['volume'].empty else 0,
                'list': sorted(df['volume'].unique().tolist()) if not df['volume'].empty else []
            },
            'metadata_completeness': {
                'with_doi': df['doi'].notna().sum() if not df['doi'].empty else 0,
                'with_abstract': df['resumo'].notna().sum() if not df['resumo'].empty else 0,
                'with_keywords': df['palavras_chave'].notna().sum() if not df['palavras_chave'].empty else 0,
                'with_affiliations': df['afiliacoes'].notna().sum() if not df['afiliacoes'].empty else 0
            },
            'sections': df['secao'].value_counts().to_dict() if not df['secao'].empty else {},
            'top_authors': df['autores'].str.split(',').explode().value_counts().head(10).to_dict() if not df['autores'].empty else {}
        }
        
        return stats


def main():
    """Main function to run the scraper."""
    print("🏥 Acta Médica Portuguesa Article Scraper")
    print("=" * 50)
    print("📋 This scraper collects academic articles from Acta Médica Portuguesa journal")
    print("🔧 Features: Concurrent processing, error handling, structured output")
    print("📊 Output: Excel (.xlsx) and CSV (.csv) files with comprehensive metadata")
    print()
    
    try:
        # Initialize scraper
        scraper = AMPScraper(
            max_workers=5,      # Number of concurrent threads
            delay=1.0,          # Delay between requests (seconds)
            retry_attempts=3    # Retry attempts for failed requests
        )
        
        print("\n🚀 Starting scraping process...")
        print("   Note: This may take a while depending on the number of articles")
        print("   Consider using max_issues parameter for testing")
        
        # Scrape all articles (use max_issues=1 for testing)
        articles_data = scraper.scrape_all_articles(max_issues=None)
        
        if articles_data:
            # Save results
            scraper.save_results(articles_data)
            
            # Print statistics
            print("\n📈 Scraping Statistics:")
            stats = scraper.get_statistics(articles_data)
            
            print(f"   📰 Total articles collected: {stats['total_articles']}")
            print(f"   📅 Date range: {stats['date_range']['earliest']} - {stats['date_range']['latest']}")
            print(f"   📚 Total volumes: {stats['volumes']['count']}")
            
            print(f"\n🔍 Metadata completeness:")
            for key, value in stats['metadata_completeness'].items():
                percentage = (value / stats['total_articles'] * 100) if stats['total_articles'] > 0 else 0
                print(f"   • {key}: {value}/{stats['total_articles']} ({percentage:.1f}%)")
            
            if stats['sections']:
                print(f"\n📂 Article sections:")
                for section, count in list(stats['sections'].items())[:5]:
                    print(f"   • {section}: {count} articles")
            
            print(f"\n✅ Scraping completed successfully!")
            print(f"📁 Check the output files: acta_medica_portuguesa_artigos.xlsx and .csv")
            
        else:
            print("❌ No articles were collected. Please check the website structure or network connectivity.")
    
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrupted by user")
    except Exception as e:
        print(f"\n🚫 Unexpected error: {e}")
        print("Please check the logs for more details")


if __name__ == "__main__":
    main()
