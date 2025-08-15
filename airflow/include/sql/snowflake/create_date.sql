CREATE TABLE IF NOT EXISTS date (
    date DATE PRIMARY KEY,
    day INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    year INTEGER,
    day_of_week INTEGER,
    day_of_week_name VARCHAR(10),
    holiday_name VARCHAR(10)
)