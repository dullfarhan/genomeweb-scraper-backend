"""
Genomile Web Scraper — FastAPI Backend
=======================================
REST API for scraping sitemaps, storing URLs in PostgreSQL via DATABASE_URL,
and serving categorised URL data to the frontend.
"""

import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from contextlib import contextmanager
from urllib.parse import urlparse, quote_plus
from typing import Optional

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

from article_scraper import ArticleScrapeRequest, scrape_article_content


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database (PostgreSQL via DATABASE_URL)
# ---------------------------------------------------------------------------

# Load environment variables from .env for local development
load_dotenv()

_db_host = os.getenv("DB_HOST")
_db_user = os.getenv("DB_USER")
_db_password = os.getenv("DB_PASSWORD")

if _db_host and _db_user and _db_password:
    # Build DATABASE_URL from DB_* (e.g. AWS RDS); password is URL-encoded for special chars
    _db_port = os.getenv("DB_PORT", "5432")
    _db_name = os.getenv("DB_NAME", "postgres")
    _password_encoded = quote_plus(_db_password)
    DATABASE_URL = (
        f"postgresql://{_db_user}:{_password_encoded}@{_db_host}:{_db_port}/{_db_name}?sslmode=require"
    )
    logger.info("Using database from DB_HOST (AWS RDS): %s", _db_host)
else:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        logging.warning(
            "DATABASE_URL is not set, defaulting to local postgres database "
            "'postgresql://postgres:postgres@localhost:5432/postgres'."
        )
        DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/postgres"
    else:
        if "render.com" in DATABASE_URL and "sslmode=" not in DATABASE_URL:
            DATABASE_URL += "&sslmode=require" if "?" in DATABASE_URL else "?sslmode=require"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS urls (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    lastmod TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_urls_category_id ON urls(category_id);
CREATE INDEX IF NOT EXISTS idx_urls_url ON urls(url);
CREATE INDEX IF NOT EXISTS idx_categories_name ON categories(name);

CREATE TABLE IF NOT EXISTS articles (
    id SERIAL PRIMARY KEY,
    url_id INTEGER NOT NULL UNIQUE REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    author TEXT,
    date_published TEXT,
    content TEXT,
    topics JSONB,
    is_premium BOOLEAN DEFAULT FALSE,
    scraped_at TIMESTAMPTZ NOT NULL
);
"""

# Connection pool: keeps connections alive and reuses them (fast, RDS-friendly)
_pool: Optional[ConnectionPool] = None


def _get_pool() -> ConnectionPool:
    if _pool is None:
        raise RuntimeError("Database pool not initialised (app not started?)")
    return _pool


@contextmanager
def get_db():
    with _get_pool().connection() as conn:
        yield conn


def init_db():
    with get_db() as conn:
        # Execute each statement separately for compatibility
        for statement in SCHEMA_SQL.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)
    logger.info("Database initialised using PostgreSQL at %s", DATABASE_URL)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class ScrapeRequest(BaseModel):
    sitemap_url: str = "https://www.genomeweb.com/sitemap.xml"


class UrlUpdate(BaseModel):
    url: Optional[str] = None
    category: Optional[str] = None


class UrlOut(BaseModel):
    id: int
    url: str
    category: str
    lastmod: Optional[str] = None
    created_at: str
    updated_at: str


class CategoryOut(BaseModel):
    id: int
    name: str
    url_count: int


class ScrapeResult(BaseModel):
    categories_created: int
    urls_inserted: int
    urls_skipped: int
    total_urls_scraped: int


class DeleteAllResult(BaseModel):
    urls_deleted: int
    categories_deleted: int

class ScrapeCategoryRequest(BaseModel):
    category_name: str
    limit: Optional[int] = None

class ScrapeCategoryResult(BaseModel):
    category_name: str
    articles_scraped: int
    errors: int
    failed_urls: list[str] = []

class ArticleOut(BaseModel):
    id: int
    url: str
    title: Optional[str] = None
    author: Optional[str] = None
    date_published: Optional[str] = None
    content: Optional[str] = None
    topics: list[str] = []
    is_premium: bool = False
    scraped_at: str


# ---------------------------------------------------------------------------
# HTTP / sitemap helpers
# ---------------------------------------------------------------------------

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def fetch_sitemap_urls(url: str) -> list[str]:
    """Fetch a sitemap index XML and return sub-sitemap <loc> URLs."""
    logger.info("Fetching sitemap index: %s", url)
    response = requests.get(url, timeout=30, headers=_BROWSER_HEADERS)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [
        loc.text.strip()
        for loc in root.findall(".//sm:sitemap/sm:loc", ns)
        if loc.text
    ]
    logger.info("Found %d sub-sitemaps in index", len(urls))
    return urls


def fetch_site_urls_from_sub_sitemap(url: str) -> list[dict[str, str]]:
    """Fetch a sub-sitemap and return list of dicts with url and lastmod."""
    logger.info("Fetching sub-sitemap: %s", url)
    response = requests.get(url, timeout=30, headers=_BROWSER_HEADERS)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    
    results = []
    for url_node in root.findall(".//sm:url", ns):
        loc = url_node.find("sm:loc", ns)
        lastmod = url_node.find("sm:lastmod", ns)
        if loc is not None and loc.text:
            results.append({
                "url": loc.text.strip(),
                "lastmod": lastmod.text.strip() if lastmod is not None and lastmod.text else None
            })
            
    logger.info("Found %d URLs in sub-sitemap %s", len(results), url)
    return results


def categorise_url(url: str) -> str:
    """Return the category key for a URL (first path segment, or 'uncategorized')."""
    parsed = urlparse(url)
    segments = [seg for seg in parsed.path.strip("/").split("/") if seg]
    return segments[0] if len(segments) >= 2 else "uncategorized"


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Genomile Scraper API",
    description="Scrape sitemaps, store & browse categorised URLs",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    global _pool
    logger.info("Opening database connection pool (RDS)...")
    _pool = ConnectionPool(
        conninfo=DATABASE_URL,
        kwargs={"row_factory": dict_row},
        min_size=2,
        max_size=20,
        max_idle=600,
        open=True,
    )
    init_db()
    logger.info("Application startup complete.")


@app.on_event("shutdown")
def on_shutdown():
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        logger.info("Database connection pool closed.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


def _build_category_to_entries(
    site_data: list[dict[str, Optional[str]]],
) -> dict[str, list[dict[str, Optional[str]]]]:
    """Group scraped entries by category. Keys = category names, values = list of {url, lastmod}."""
    by_category: dict[str, list[dict[str, Optional[str]]]] = {}
    for entry in site_data:
        cat_name = categorise_url(entry["url"])
        by_category.setdefault(cat_name, []).append(
            {"url": entry["url"], "lastmod": entry.get("lastmod")}
        )
    return by_category


@app.post("/api/scrape", response_model=ScrapeResult)
def scrape_sitemap(body: ScrapeRequest):
    """Scrape a sitemap URL, categorise URLs, clear DB, then bulk-insert categories and URLs."""
    t0 = time.perf_counter()
    logger.info("[scrape] started at %s", datetime.now(timezone.utc).isoformat())

    try:
        sub_sitemaps = fetch_sitemap_urls(body.sitemap_url)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch sitemap index: {exc}")

    site_data: list[dict[str, Optional[str]]] = []
    for sm_url in sub_sitemaps:
        try:
            site_data.extend(fetch_site_urls_from_sub_sitemap(sm_url))
        except Exception as exc:
            logger.warning("Skipping sub-sitemap %s: %s", sm_url, exc)

    t_fetch = time.perf_counter() - t0
    logger.info("[scrape] fetched %d URLs in %.2fs", len(site_data), t_fetch)

    # Build dict: category_name -> [{"url", "lastmod"}, ...]
    by_category = _build_category_to_entries(site_data)
    category_names = sorted(by_category.keys())
    categories_created = len(category_names)
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        # 1) Clear database first
        conn.execute("DELETE FROM urls")
        conn.execute("DELETE FROM categories")
        logger.info("Cleared existing urls and categories")

        if not category_names:
            logger.info("No categories to insert")
            return ScrapeResult(
                categories_created=0,
                urls_inserted=0,
                urls_skipped=0,
                total_urls_scraped=len(site_data),
            )

        # 2) Bulk insert all categories and get id per name
        placeholders = ", ".join(
            "(%s, %s)" for _ in category_names
        )
        params = []
        for name in category_names:
            params.extend([name, now])
        rows = conn.execute(
            f"INSERT INTO categories (name, created_at) VALUES {placeholders} RETURNING id, name",
            params,
        ).fetchall()
        name_to_id = {r["name"]: r["id"] for r in rows}

        # 3) Bulk insert all URLs — deduplicate by URL (sitemap can list same URL twice)
        seen_urls: set[str] = set()
        url_rows: list[tuple[str, int, Optional[str], str, str]] = []
        for cat_name in category_names:
            cat_id = name_to_id[cat_name]
            for entry in by_category[cat_name]:
                u = entry["url"]
                if u not in seen_urls:
                    seen_urls.add(u)
                    url_rows.append((u, cat_id, entry.get("lastmod"), now, now))

        urls_skipped_dupes = len(site_data) - len(url_rows)
        if urls_skipped_dupes:
            logger.info("[scrape] skipped %d duplicate URLs within sitemap", urls_skipped_dupes)

        if not url_rows:
            logger.info("No URLs to insert (all duplicates)")
            return ScrapeResult(
                categories_created=categories_created,
                urls_inserted=0,
                urls_skipped=urls_skipped_dupes,
                total_urls_scraped=len(site_data),
            )

        # Insert in chunks to avoid huge single statement (e.g. 500 per batch)
        BATCH_SIZE = 500
        urls_inserted = 0
        for i in range(0, len(url_rows), BATCH_SIZE):
            batch = url_rows[i : i + BATCH_SIZE]
            placeholders = ", ".join(
                "(%s, %s, %s, %s, %s)" for _ in batch
            )
            params = []
            for row in batch:
                params.extend(row)
            conn.execute(
                f"""INSERT INTO urls (url, category_id, lastmod, created_at, updated_at)
                    VALUES {placeholders}
                    ON CONFLICT (url) DO NOTHING""",
                params,
            )
            urls_inserted += len(batch)

    t_total = time.perf_counter() - t0
    t_db = t_total - t_fetch
    logger.info(
        "[scrape] finished at %s — %d categories, %d urls inserted (total: %d) | db: %.2fs | total: %.2fs",
        datetime.now(timezone.utc).isoformat(),
        categories_created,
        urls_inserted,
        len(site_data),
        t_db,
        t_total,
    )

    return ScrapeResult(
        categories_created=categories_created,
        urls_inserted=urls_inserted,
        urls_skipped=urls_skipped_dupes,
        total_urls_scraped=len(site_data),
    )


@app.get("/api/categories", response_model=list[CategoryOut])
def list_categories():
    """List all categories with their URL counts."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT c.id, c.name, COUNT(u.id) AS url_count
            FROM categories c
            LEFT JOIN urls u ON u.category_id = c.id
            GROUP BY c.id
            ORDER BY c.name
            """
        ).fetchall()
    return [CategoryOut(id=r["id"], name=r["name"], url_count=r["url_count"]) for r in rows]


@app.get("/api/urls", response_model=list[UrlOut])
def list_urls(
    category: Optional[str] = Query(None, description="Filter by category name"),
    search: Optional[str] = Query(None, description="Search within URL text"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
):
    """List URLs with optional category filter, search, and pagination."""
    offset = (page - 1) * limit
    conditions: list[str] = []
    params: list = []

    if category:
        conditions.append("c.name = %s")
        params.append(category)
    if search:
        conditions.append("u.url LIKE %s")
        params.append(f"%{search}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
        SELECT u.id, u.url, c.name AS category, u.lastmod, u.created_at, u.updated_at
        FROM urls u
        JOIN categories c ON c.id = u.category_id
        {where}
        ORDER BY u.id DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()

    return [
        UrlOut(
            id=r["id"],
            url=r["url"],
            category=r["category"],
            lastmod=r["lastmod"],
            created_at=r["created_at"],
            updated_at=r["updated_at"],
        )
        for r in rows
    ]


@app.get("/api/urls/{url_id}", response_model=UrlOut)
def get_url(url_id: int):
    """Get a single URL entry by ID."""
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT u.id, u.url, c.name AS category, u.lastmod, u.created_at, u.updated_at
            FROM urls u
            JOIN categories c ON c.id = u.category_id
            WHERE u.id = %s
            """,
            (url_id,),
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="URL not found")

    return UrlOut(
        id=row["id"],
        url=row["url"],
        category=row["category"],
        lastmod=row["lastmod"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@app.put("/api/urls/{url_id}", response_model=UrlOut)
def update_url(url_id: int, body: UrlUpdate):
    """Update a URL entry (url text and/or category)."""
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        existing = conn.execute(
            "SELECT * FROM urls WHERE id = %s",
            (url_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="URL not found")

        new_url = body.url if body.url else existing["url"]
        cat_id = existing["category_id"]

        if body.category:
            cat_row = conn.execute(
                "SELECT id FROM categories WHERE name = %s",
                (body.category,),
            ).fetchone()
            if cat_row:
                cat_id = cat_row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO categories (name, created_at) VALUES (%s, %s) RETURNING id",
                    (body.category, now),
                )
                cat_id = cur.fetchone()["id"]

        try:
            conn.execute(
                "UPDATE urls SET url = %s, category_id = %s, updated_at = %s WHERE id = %s",
                (new_url, cat_id, now, url_id),
            )
        except psycopg.IntegrityError:
            raise HTTPException(status_code=409, detail="URL already exists")

        row = conn.execute(
            """
            SELECT u.id, u.url, c.name AS category, u.lastmod, u.created_at, u.updated_at
            FROM urls u
            JOIN categories c ON c.id = u.category_id
            WHERE u.id = %s
            """,
            (url_id,),
        ).fetchone()

    return UrlOut(
        id=row["id"],
        url=row["url"],
        category=row["category"],
        lastmod=row["lastmod"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@app.delete("/api/urls/{url_id}")
def delete_url(url_id: int):
    """Delete a URL entry by ID."""
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM urls WHERE id = %s",
            (url_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="URL not found")
        conn.execute(
            "DELETE FROM urls WHERE id = %s",
            (url_id,),
        )
    return {"detail": "Deleted", "id": url_id}


@app.delete("/api/urls", response_model=DeleteAllResult)
def delete_all_urls():
    """Delete all URLs and categories from the database."""
    with get_db() as conn:
        url_count = conn.execute("SELECT COUNT(*) AS c FROM urls").fetchone()["c"]
        cat_count = conn.execute("SELECT COUNT(*) AS c FROM categories").fetchone()["c"]

        conn.execute("DELETE FROM urls")
        conn.execute("DELETE FROM categories")

    logger.warning("Deleted all data: %d urls and %d categories", url_count, cat_count)
    return DeleteAllResult(urls_deleted=url_count, categories_deleted=cat_count)


@app.get("/api/data")
def get_grouped_data():
    """
    Return all URLs grouped by category — same shape as the original routes.json.
    Response: { "category_name": ["url1", "url2", ...], ... }
    """
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT c.name AS category, u.url, u.lastmod,
                   CASE WHEN a.id IS NOT NULL THEN true ELSE false END AS is_scraped
            FROM urls u
            JOIN categories c ON c.id = u.category_id
            LEFT JOIN articles a ON a.url_id = u.id
            ORDER BY c.name, u.id
            """
        ).fetchall()

    result: dict[str, list[dict[str, Optional[str]]]] = {}
    for r in rows:
        result.setdefault(r["category"], []).append(
            {"url": r["url"], "lastmod": r["lastmod"], "is_scraped": r["is_scraped"]}
        )
    return result

@app.post("/api/scrape-article")
async def scrape_article(body: ArticleScrapeRequest):
    """
    Scrape the content of a specific article URL.
    """
    result = await scrape_article_content(body.url)
    if not result["success"]:
        raise HTTPException(status_code=502, detail=result["error"])
    return result


import json
import asyncio
import random

@app.post("/api/scrape-category", response_model=ScrapeCategoryResult)
async def scrape_category(body: ScrapeCategoryRequest):
    """
    Scrape all articles within a specific category and store them in the database.
    """
    logger.info("Starting scrape run for category: %s", body.category_name)
    
    with get_db() as conn:
        # Fetch category id
        cat_row = conn.execute("SELECT id FROM categories WHERE name = %s", (body.category_name,)).fetchone()
        if not cat_row:
            raise HTTPException(status_code=404, detail="Category not found")
        
        # Fetch URLs for this category
        query = "SELECT id, url FROM urls WHERE category_id = %s ORDER BY id DESC"
        params = [cat_row["id"]]
        
        if body.limit:
            query += " LIMIT %s"
            params.append(body.limit)
            
        urls = conn.execute(query, params).fetchall()

    if not urls:
        return ScrapeCategoryResult(category_name=body.category_name, articles_scraped=0, errors=0)

    articles_scraped = 0
    errors = 0
    failed_urls = []
    now = datetime.now(timezone.utc).isoformat()

    for url_row in urls:
        url_id = url_row["id"]
        url_str = url_row["url"]
        
        try:
            # Add delay to avoid hammering the server
            delay = random.uniform(2, 3)
            logger.info(f"Waiting {delay:.2f}s before scraping...")
            await asyncio.sleep(delay)
            
            # Scrape content
            result = await scrape_article_content(url_str)
            if not result["success"]:
                logger.warning("Failed to scrape URL %s: %s", url_str, result["error"])
                errors += 1
                failed_urls.append(url_str)
                continue
                
            data = result["article_data"]
            topics_json = json.dumps(data.get("topics", []))
            
            # Store in database
            with get_db() as conn:
                conn.execute(
                    """
                    INSERT INTO articles (url_id, title, author, date_published, content, topics, is_premium, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        author = EXCLUDED.author,
                        date_published = EXCLUDED.date_published,
                        content = EXCLUDED.content,
                        topics = EXCLUDED.topics,
                        is_premium = EXCLUDED.is_premium,
                        scraped_at = EXCLUDED.scraped_at
                    """,
                    (
                        url_id, 
                        data.get("title"), 
                        data.get("author"), 
                        data.get("date"), 
                        data.get("content"), 
                        topics_json, 
                        data.get("is_premium", False), 
                        now
                    )
                )
            logger.info("Successfully scraped and saved: %s", url_str)
            articles_scraped += 1
        except Exception as exc:
            logger.error("Error processing URL %s: %s", url_str, exc)
            errors += 1
            failed_urls.append(url_str)

    return ScrapeCategoryResult(
        category_name=body.category_name,
        articles_scraped=articles_scraped,
        errors=errors,
        failed_urls=failed_urls
    )


@app.delete("/api/scraped-articles/{category_name}")
def delete_scraped_articles_for_category(category_name: str):
    """Delete all scraped articles for a specific category."""
    with get_db() as conn:
        cat_row = conn.execute("SELECT id FROM categories WHERE name = %s", (category_name,)).fetchone()
        if not cat_row:
            raise HTTPException(status_code=404, detail="Category not found")
            
        cur = conn.execute(
            """
            WITH deleted AS (
                DELETE FROM articles a
                USING urls u
                WHERE a.url_id = u.id AND u.category_id = %s
                RETURNING a.id
            )
            SELECT COUNT(*) AS c FROM deleted
            """,
            (cat_row["id"],)
        )
        count = cur.fetchone()["c"]
        
    logger.warning("Deleted %d scraped articles for category %s", count, category_name)
    return {"detail": f"Deleted scraped articles for {category_name}", "articles_deleted": count}

@app.get("/api/scraped-articles", response_model=list[ArticleOut])
def list_scraped_articles(
    category: Optional[str] = Query(None, description="Filter by category name"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
):
    """
    List all scraped articles stored in the database.
    """
    offset = (page - 1) * limit
    conditions = []
    params = []

    if category:
        conditions.append("c.name = %s")
        params.append(category)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
        SELECT 
            a.id, u.url, a.title, a.author, a.date_published, 
            a.content, a.topics, a.is_premium, a.scraped_at
        FROM articles a
        JOIN urls u ON a.url_id = u.id
        JOIN categories c ON u.category_id = c.id
        {where}
        ORDER BY a.scraped_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()

    result = []
    for r in rows:
        topics_list = []
        if r["topics"]:
            topics_list = r["topics"] if isinstance(r["topics"], list) else json.loads(r["topics"])
            
        result.append(ArticleOut(
            id=r["id"],
            url=r["url"],
            title=r["title"],
            author=r["author"],
            date_published=r["date_published"],
            content=r["content"],
            topics=topics_list,
            is_premium=r["is_premium"],
            scraped_at=str(r["scraped_at"])
        ))

    return result


@app.get("/api/scraped-article")
def get_single_scraped_article(url: str):
    """Fetch a single scraped article from the database by its URL."""
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT a.title, a.author, a.date_published, a.content, a.topics, a.is_premium
            FROM articles a
            JOIN urls u ON a.url_id = u.id
            WHERE u.url = %s
            """,
            (url,)
        ).fetchone()

    if not row:
        return {"success": False, "error": "Article not found in database. Scrape it first."}

    article_data = {
        "title": row["title"],
        "date": row["date_published"],
        "author": row["author"] or "",
        "content": row["content"],
        "topics": row["topics"] or [],
        "is_premium": row["is_premium"],
        "url": url,
    }
    
    return {"success": True, "article_data": article_data}

