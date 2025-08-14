CREATE TABLE IF NOT EXISTS logs (
    run_id VARCHAR(36) PRIMARY KEY,       
    ingested_at DATE,
    source_updated_on DATE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(10),
    mode VARCHAR(10),
    config TEXT
)