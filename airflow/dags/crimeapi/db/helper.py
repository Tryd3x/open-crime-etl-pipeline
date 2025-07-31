import logging 
from datetime import datetime, timezone
from sqlalchemy.engine.url import URL
from sqlalchemy import MetaData, insert, update, select, func
from sqlalchemy.engine.base import Engine

logger = logging.getLogger(__name__)

def initialize_run_log(engine: Engine, config: dict) -> int:
    logger.info("Initializing pipeline status to 'pipeline_logs'")
    meta = MetaData()
    meta.reflect(engine)

    log_table = meta.tables['pipeline_logs']

    with engine.begin() as conn:
        # Initiate log
        st1 = insert(log_table).values(
            ingested_at = datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            start_time = datetime.now(timezone.utc).strftime("%H:%M:%S"),
            status = 'RUNNING',
            config = str(config)
        ).returning(log_table.c.run_id)

        result = conn.execute(st1)
        run_id = result.scalar()

        # Fetch source_updated_on
        st2 = select(func.max(log_table.c.source_updated_on))
        result = conn.execute(st2)
        last_updated = result.scalar()
        return (run_id, last_updated)
    
def update_run_log(engine: Engine, run_id: int, status : str, file_location: str = None, source_updated_on: datetime = None) -> None:
    logger.info(f"Updating pipeline status to '{status}' for run_id={run_id}")

    meta = MetaData()
    meta.reflect(engine)
    log_table = meta.tables['pipeline_logs']

    values = {
        'end_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
        'status' : status
    }

    if status.upper() == "SUCCESS":
        values.update({
            'file_location' : file_location,
            'source_updated_on' : source_updated_on
        })
        

    with engine.begin() as conn:
        st = update(log_table).where(log_table.c.run_id == run_id).values(**values)
        conn.execute(st)