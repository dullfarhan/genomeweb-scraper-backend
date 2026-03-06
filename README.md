## GenomeWeb sitemap scraper

This small script downloads the GenomeWeb sitemap, extracts all page URLs, and saves them in two JSON files for later processing.

### How data is saved

- **`site_urls.json`**  
  - Type: JSON array of strings.  
  - Each item is a full page URL taken directly from the sub-sitemaps under `https://www.genomeweb.com/sitemap.xml`.  
  - Example:
    ```json
    [
      "https://www.genomeweb.com/genetic-research/example-article",
      "https://www.genomeweb.com/cancer/example-article-2"
    ]
    ```

- **`routes.json`**  
  - Type: JSON object (dictionary).  
  - Keys are **category names**, values are arrays of URLs in that category.  
  - Category rule:
    - Take the URL path (without protocol and domain), strip leading/trailing `/`, and split on `/`.
    - If there are **at least two non-empty segments**, the **first segment** is the category key.  
      - Example: `/cancer/example-article` → path segments `["cancer", "example-article"]` → key `"cancer"`.  
    - If there are **0 or 1 segments**, the URL is grouped under the key `"uncategorized"`.  
      - Example: `/about` → path segments `["about"]` → key `"uncategorized"`.
  - Example structure:
    ```json
    {
      "cancer": [
        "https://www.genomeweb.com/cancer/example-article",
        "https://www.genomeweb.com/cancer/another-article"
      ],
      "genetic-research": [
        "https://www.genomeweb.com/genetic-research/example-article"
      ],
      "uncategorized": [
        "https://www.genomeweb.com/about",
        "https://www.genomeweb.com/contact"
      ]
    }
    ```

### How to run

```bash
pip install requests
python main.py
```

## Backend API & Cloudflare D1

The `backend/` folder also contains a FastAPI app (`main.py`) that exposes a REST API for scraping sitemaps and storing URLs in a SQLite-compatible schema (the same schema used by Cloudflare D1).

### D1 integration

- `wrangler.toml` defines a D1 database binding:
  - Binding name: `DB`
  - Database name: `genome-web-scrape`
- `d1_migration.sql` contains the SQL schema for the `categories` and `urls` tables and their indexes.

To create and migrate the D1 database:

```bash
wrangler d1 create genome-web-scrape
wrangler d1 execute genome-web-scrape --file=./d1_migration.sql
```

