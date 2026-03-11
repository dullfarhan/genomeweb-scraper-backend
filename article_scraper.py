import asyncio
from typing import Optional
from playwright.async_api import async_playwright
import playwright_stealth
from pydantic import BaseModel

class ArticleScrapeRequest(BaseModel):
    url: str

async def scrape_article_content(url: str) -> dict:
    """
    Fetch and scrape a GenomeWeb article for its main content using Playwright.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            java_script_enabled=True,
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()
        await playwright_stealth.stealth_async(page)
        
        try:
            # Navigate to the URL and wait for network to settle
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            # Check for bot block or empty page
            page_title = await page.title()
            if "Access Denied" in page_title or "403 Forbidden" in page_title:
                await browser.close()
                return {
                    "success": False,
                    "url": url,
                    "content": "",
                    "error": f"Site Blocked: {page_title}"
                }


            # 1. Fetch raw HTML
            html_content = await page.content()
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 2. Extract Metadata
            title_el = soup.select_one('.article-header .h1, h1')
            title = title_el.get_text(strip=True) if title_el else (soup.title.string if soup.title else "")
            
            byline_box = soup.select_one('.article-header__byline_container')
            date_str = ""
            author_str = ""
            if byline_box:
                children = byline_box.find_all(recursive=False)
                if children:
                    date_str = children[0].get_text(strip=True)
                    authors = [child.get_text(strip=True) for child in children[1:]]
                    author_str = ", ".join([a for a in authors if a and a != '|'])
            
            # 3. Extract Content
            content = ""
            text_long = soup.select_one('.article-content .text-long') or soup.select_one('.body .text-long') or soup.select_one('.text-long')
            
            if text_long:
                paragraphs = text_long.find_all('p')
                if paragraphs:
                    content = "\\n\\n".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                else:
                    content = text_long.get_text(strip=True)
            else:
                article_body = soup.select_one('.article-content') or soup.select_one('article') or soup.select_one('.body')
                if article_body:
                    paragraphs = article_body.find_all('p')
                    if paragraphs:
                        content = "\\n\\n".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                    else:
                        content = article_body.get_text(strip=True)

            # 4. Extract Topics
            topics = []
            topic_elements = soup.select('.topics-list .mytopics-combo-link__link')
            if topic_elements:
                topics = [el.get_text(strip=True) for el in topic_elements if el.get_text(strip=True)]

            # 5. Extract Premium Badge
            is_premium = False
            premium_badge = soup.select_one('.article-header__labels .badge__text')
            if premium_badge and 'Premium' in premium_badge.get_text(strip=True):
                is_premium = True

            article_data = {
                'title': title,
                'date': date_str,
                'author': author_str,
                'content': content,
                'topics': topics,
                'is_premium': is_premium
            }

            # Debugging print
            print(f"Scraped {url}: Title='{article_data['title']}', Content Length={len(article_data['content'])}, Topics={len(article_data['topics'])}, Premium={article_data['is_premium']}")

            await browser.close()
            
            if not article_data['content']:
                article_data['content'] = "No content found in the expected containers."

            return {
                "success": True,
                "url": url,
                "article_data": article_data,
                "error": None
            }


        except Exception as e:
            await browser.close()
            return {
                "success": False,
                "url": url,
                "content": "",
                "error": str(e)
            }
