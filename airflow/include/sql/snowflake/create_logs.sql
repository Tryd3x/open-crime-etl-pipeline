CREATE TABLE IF NOT EXISTS logs (
    run_id TEXT PRIMARY KEY, 
    load_date DATE, 
    type VARCHAR(10),
    mode VARCHAR(10), 
    start_time TIME, 
    end_time TIME, 
    status VARCHAR(10), 
    config TEXT,
    PRIMARY KEY (run_id, load_date)
)