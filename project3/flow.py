"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python flow.py               # data/raw, full data      -> bluesky.duckdb
  python flow.py data/sample   # the committed sample     -> bluesky_sample.duckdb
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

# the folder to read comes from the command line so the grader can run the sample
SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))  # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"bluesky{TAG}.duckdb"  # bluesky_sample.duckdb for the sample
CHART = f"charts/reply_share_by_language{TAG}.png"  # the sample never overwrites my chart

# names for the language codes bluesky uses, anything else shows its code
LANGUAGES = {"en": "English", "ja": "Japanese", "pt": "Portuguese", "de": "German",
             "es": "Spanish", "fr": "French", "ko": "Korean", "it": "Italian",
             "nl": "Dutch", "zh": "Chinese", "ru": "Russian", "tr": "Turkish"}


# function to copy every landed file into one raw table
@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of bluesky.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


# function to keep one copy of each kafka record and drop what is not a post event
@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed'
              AND kind = 'commit'                   -- not identity or account events
              AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}


# function to keep one row per post with its latest operation
@task
def entities():
    """Kind 2: one row per post (did, rkey), with its latest operation."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation,
                   lower(split_part(lang, '-', 1)) AS lang,   -- en-US and en-GB count as en
                   text,
                   make_timestamp(time_us) AS event_time,      -- microseconds -> UTC
                   (raw::JSON ->> '$.data.commit.record.reply.parent.uri')
                       IS NOT NULL AS is_reply                -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey
                                       ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM posts").fetchone()[0]


# function to compare the share of replies across the five most common languages
@task
def marts():
    with duckdb.connect(DB) as con:
        # only posts that still exist, a deleted post has no text or language left
        # ties in the top 5 are broken by the code so every run picks the same five
        con.execute("""
            CREATE OR REPLACE TABLE mart_reply_share AS
            WITH created AS (
                SELECT lang, is_reply FROM posts WHERE operation = 'create'
            ),
            top5 AS (
                SELECT lang FROM created WHERE lang IS NOT NULL
                GROUP BY lang ORDER BY count(*) DESC, lang LIMIT 5
            )
            SELECT lang,
                   count(*) AS posts,
                   count(*) FILTER (WHERE is_reply) AS replies,
                   round(100.0 * count(*) FILTER (WHERE is_reply) / count(*), 1) AS reply_pct,
                   round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_of_top5,
                   (SELECT round(100.0 * avg(is_reply::INT), 1) FROM created) AS all_reply_pct
            FROM created JOIN top5 USING (lang)
            GROUP BY lang
            ORDER BY reply_pct DESC, lang
        """)
        return con.execute("SELECT count(*) FROM mart_reply_share").fetchone()[0]


# function to draw the mart as one bar per language
@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("SELECT lang, posts, reply_pct, all_reply_pct FROM mart_reply_share").fetchall()
        start, end = con.execute("SELECT min(event_time), max(event_time) FROM posts").fetchone()
    os.makedirs("charts", exist_ok=True)
    # highest share at the top of the chart
    rows = rows[::-1]
    labels = [f"{LANGUAGES.get(r[0], r[0])} ({r[1]:,} posts)" for r in rows]
    shares = [r[2] for r in rows]
    overall = rows[0][3] if rows else 0
    plt.figure(figsize=(8, 4))
    plt.barh(labels, shares, color="#2a7fb8")
    plt.axvline(overall, color="#444444", linestyle="--", label=f"all languages: {overall}%")
    for y, s in enumerate(shares):
        plt.text(s + 0.5, y, f"{s}%", va="center")
    # the title states the finding, the subtitle the data it comes from
    if rows:
        top, low = rows[-1], rows[0]
        plt.title(f"{LANGUAGES.get(top[0], top[0])} posts are replies most often ({top[2]}%), "
                  f"{LANGUAGES.get(low[0], low[0])} least ({low[2]}%)\n"
                  f"Bluesky, {start:%d %b %H:%M} to {end:%d %b %H:%M} UTC, duplicates removed",
                  fontsize=10)
    plt.xlabel("replies (% of posts in that language)")
    plt.xlim(0, 100)
    plt.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()
    return CHART


# main flow: load, stage, entities, marts, chart, then log the four numbers
@flow(name="bluesky-pipeline")
def bluesky_pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    posts = entities()
    marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique posts:              %d", posts)
    log.info("Chart: %s", path)


if __name__ == "__main__":
    bluesky_pipeline()
