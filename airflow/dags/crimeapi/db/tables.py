from sqlalchemy import MetaData, Table, Column, Integer, Float, Date, DateTime, TIME, String, Text
from sqlalchemy.engine.base import Engine
import logging

logger = logging.getLogger(__name__)

def create_log_table(engine: Engine) -> Table:
    metadata = MetaData()
    table = Table(
        "pipeline_logs", metadata,
        Column("run_id", Integer, primary_key=True, autoincrement=True),
        Column("ingested_at", Date),
        Column("source_updated_on", Date),
        Column("start_time", TIME),
        Column("end_time", TIME),
        Column("status", String(10)),
        Column("config", Text),
    )

    logger.info("Creating Table 'pipeline_logs'")
    metadata.create_all(engine, checkfirst=True)
    return table

def create_date_table(engine: Engine) -> Table:
    meta = MetaData()
    table = Table(
        "date", meta,
        Column('date', Date, primary_key=True),
        Column('day', Integer),
        Column('month', Integer),
        Column('month_name', String),
        Column('year', Integer),
        Column('day_of_week', Integer),
        Column('day_of_week_name', String),
        Column('holiday_name', String)
    )

    logger.info("Creating Table 'date'")
    meta.create_all(bind=engine)
    return table

def create_crime_table(engine: Engine) -> Table:
    meta = MetaData()
    table = Table(
        "crime", meta, 
        Column("crime_id", String, primary_key=True),
        Column("case", String),
        Column("date_of_occurrence", DateTime(timezone="US/Central")),
        Column("block", String),
        Column("iucr", String),
        Column("primary_description", String),
        Column("secondary_description", String),
        Column("location_description", String),
        Column("arrest", String),
        Column("domestic", String),
        Column("beat", Integer),
        Column("district", Integer),
        Column("ward", Integer),
        Column("community_area", Integer),
        Column("fbi_code", String),
        Column("x_coordinate", Integer),
        Column("y_coordinate", Integer),
        Column("latitude", Float),
        Column("longitude", Float),
        Column("source_updated_on", DateTime(timezone=True)),
    )
    
    logger.info("Creating Table: 'crime'")
    meta.create_all(bind=engine, checkfirst=True) # does not re-create table if it already exists
    return table