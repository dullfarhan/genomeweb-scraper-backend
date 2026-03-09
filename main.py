"""
Genomile Web Scraper — FastAPI Backend
=======================================
REST API for scraping sitemaps, storing URLs in PostgreSQL via DATABASE_URL,
and serving categorised URL data to the frontend.
"""

import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from contextlib import contextmanager
from urllib.parse import urlparse
from typing import Optional

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

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

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    # This makes local development easier but you should override in production
    logging.warning(
        "DATABASE_URL is not set, defaulting to local postgres database "
        "'postgresql://postgres:postgres@localhost:5432/postgres'."
    )
    DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/postgres"


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
"""


def _get_connection() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


@contextmanager
def get_db():
    conn = _get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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


# ---------------------------------------------------------------------------
# Sitemap helpers (ported from original script)
# ---------------------------------------------------------------------------
def fetch_sitemap_urls(url: str) -> list[str]:
    """Fetch a sitemap index XML and return sub-sitemap <loc> URLs."""
    logger.info("Fetching sitemap index: %s", url)
    response = requests.get(url, timeout=30)
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
    response = requests.get(url, timeout=30)
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
    logger.info("Connecting to database...")
    init_db()
    logger.info("Application startup complete.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/scrape", response_model=ScrapeResult)
def scrape_sitemap(body: ScrapeRequest):
    """Scrape a sitemap URL, categorise URLs, and store them in the DB."""
    try:
        sub_sitemaps = fetch_sitemap_urls(body.sitemap_url)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch sitemap index: {exc}")

    site_data: list[dict[str, str]] = []
    for sm_url in sub_sitemaps:
        try:
            site_data.extend(fetch_site_urls_from_sub_sitemap(sm_url))
        except Exception as exc:
            logger.warning("Skipping sub-sitemap %s: %s", sm_url, exc)

    logger.info("Collected %d site URLs total", len(site_data))

    categories_created = 0
    urls_inserted = 0
    urls_skipped = 0
    now = datetime.now(timezone.utc).isoformat()
    logger.info("Start saving in DB")
    with get_db() as conn:
        for entry in site_data:
            logger.info("entry to save %s: ",entry)
            url = entry["url"]
            lastmod = entry["lastmod"]
            cat_name = categorise_url(url)

            # Upsert category
            row = conn.execute(
                "SELECT id FROM categories WHERE name = %s",
                (cat_name,),
            ).fetchone()
            if row:
                cat_id = row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO categories (name, created_at) VALUES (%s, %s) RETURNING id",
                    (cat_name, now),
                )
                cat_id = cur.fetchone()["id"]
                categories_created += 1
                logger.info("Created category: %s", cat_name)

            # Insert URL (skip duplicates)
            try:
                conn.execute(
                    "INSERT INTO urls (url, category_id, lastmod, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
                    (url, cat_id, lastmod, now, now),
                )
                urls_inserted += 1
                logger.debug("Inserted URL: %s", url)
            except psycopg.IntegrityError:
                urls_skipped += 1
                logger.debug("Skipped duplicate URL: %s", url)

    logger.info(
        "Saved scrape data to DB at %s — %d inserted, %d skipped, %d categories created",
        now,
        urls_inserted,
        urls_skipped,
        categories_created,
    )

    return ScrapeResult(
        categories_created=categories_created,
        urls_inserted=urls_inserted,
        urls_skipped=urls_skipped,
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
            SELECT c.name AS category, u.url, u.lastmod
            FROM urls u
            JOIN categories c ON c.id = u.category_id
            ORDER BY c.name, u.id
            """
        ).fetchall()

    result: dict[str, list[dict[str, str | None]]] = {}
    for r in rows:
        result.setdefault(r["category"], []).append(
            {"url": r["url"], "lastmod": r["lastmod"]}
        )
    return result
