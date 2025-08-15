CREATE TABLE IF NOT EXISTS logs (
    run_id TEXT PRIMARY KEY,       
    ingested_at DATE,
    source_updated_on DATE,
    start_time TIME,
    end_time TIME,
    status VARCHAR(10),
    mode VARCHAR(10),
    config TEXT
)