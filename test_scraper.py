import asyncio
from article_scraper import scrape_article_content
import json

async def main():
    url = "https://www.genomeweb.com/molecular-diagnostics/who-recommend-new-class-near-patient-tests-tongue-swabs-pooled-samples-tb"
    result = await scrape_article_content(url)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
