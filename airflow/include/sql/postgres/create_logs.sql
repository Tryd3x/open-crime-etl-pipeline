CREATE TABLE IF NOT EXISTS logs (
    run_id TEXT,       
    load_date DATE,
    type VARCHAR(10),
    mode VARCHAR(10),
    status VARCHAR(10),
    start_time TIME,
    end_time TIME,
    config TEXT,
    PRIMARY KEY (run_id, load_date)
)