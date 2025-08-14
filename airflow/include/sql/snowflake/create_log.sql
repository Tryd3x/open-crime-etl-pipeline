CREATE TABLE pipeline_logs (
    run_id VARCHAR(10) PRIMARY KEY, 
    ingested_at DATE, 
    source_updated_on DATE, 
    start_time TIME, 
    end_time TIME, 
    status VARCHAR(10), 
    mode VARCHAR(10), 
    config TEXT
)