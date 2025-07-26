import logging 
from datetime import datetime, timezone
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date, TIME, Text, insert, update

logger = logging.getLogger(__name__)

def create_postgres_conn(
        host: str,
        port: str,
        username: str,
        password: str,
        db: str,
):
    url = URL.create(
        drivername='postgresql',
        host=host,
        port=port,
        username=username,
        password=password,
        database=db
    )

    return create_engine(url)

def create_log_table(engine):
    metadata = MetaData()
    table = Table(
        "pipeline_logs", metadata,
        Column("run_id", Integer, primary_key=True, autoincrement=True),
        Column("ingested_at", Date),
        Column("start_time", TIME),
        Column("end_time", TIME),
        Column("status", String(10)),
        Column("batch_count", Integer),
        Column("batch_size", Integer),
        Column("config", Text),
        Column("file_location", Text)
    )

    logger.info("Creating Table 'pipeline_logs'")
    metadata.create_all(engine, checkfirst=True)
    return table

def initialize_run_log(engine, config: dict):
    logger.info("Initializing pipeline status to 'pipeline_logs'")
    meta = MetaData()
    meta.reflect(engine)

    log_table = meta.tables['pipeline_logs']

    with engine.begin() as conn:
        st = insert(log_table).values(
            ingested_at = datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            start_time = datetime.now(timezone.utc).strftime("%H:%M:%S"),
            status = 'RUNNING',
            batch_size = config.get('batchsize'),
            config = str(config)
        ).returning(log_table.c.run_id)

        result = conn.execute(st)
        run_id = result.scalar()
        return run_id
    
def update_run_log(engine, run_id: int, status : str, batch_count: int = None, file_location: str = None):
    logger.info(f"Updating pipeline status to '{status}' for run_id={run_id}")

    meta = MetaData()
    meta.reflect(engine)
    log_table = meta.tables['pipeline_logs']

    values = {
        'end_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
        'status' : status
    }

    if status.upper() == "SUCCESS":
        # Add batch count and file_location
        values.update({
            'batch_count' : batch_count,
            'file_location' : file_location
        })
        

    with engine.begin() as conn:
        st = update(log_table).where(log_table.c.run_id == run_id).values(**values)

        conn.execute(st)