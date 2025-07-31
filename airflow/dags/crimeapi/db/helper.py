import logging 
from datetime import datetime, timezone
from sqlalchemy.engine.url import URL
from sqlalchemy import MetaData, insert, update
from sqlalchemy.engine.base import Engine

logger = logging.getLogger(__name__)

def initialize_run_log(engine: Engine, config: dict) -> int:
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
    
def update_run_log(engine: Engine, run_id: int, status : str, batch_count: int = None, file_location: str = None, source_updated_on: datetime = None) -> None:
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
            'batch_count' : batch_count,
            'file_location' : file_location,
            'source_updated_on' : source_updated_on
        })
        

    with engine.begin() as conn:
        st = update(log_table).where(log_table.c.run_id == run_id).values(**values)
        conn.execute(st)