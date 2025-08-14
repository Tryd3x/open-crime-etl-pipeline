import logging
from sqlalchemy import text
from datetime import datetime, timezone
from crimeapi.db.snowflake.db_snowflake import DBSnowflake

logger = logging.getLogger(__name__)
    
def init_run_log(engine, run_id:str, config: dict) -> int:
    """ Initialize run and fetch run_id """
    
    with engine.begin() as conn:
        values = {
            'run_id' : run_id,
            'ingested_at' : datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'start_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
            'status' : 'RUNNING',
            'config' : str(config)
        }

        val_clause = ", ".join([f":{k}" for k in values.keys()])

        query = text(f"""
            INSERT INTO pipeline_logs(
                run_id,
                ingested_at, 
                start_time,
                status,
                config
            )
            VALUES ({val_clause})
        """)

        conn.execute(query, values)

def get_last_source_updated_on_from_log(engine):
    """ Fetch latest date the source was updated"""

    with engine.begin() as conn:
        query = """SELECT MAX(source_updated_on) FROM pipeline_logs"""

        last_source_update = conn.execute(query).scalar()
        return last_source_update

def get_last_ingest_date_from_log(engine):
    """ Fetch the last load date"""
    with engine.begin() as conn:
        query = """
            SELECT MAX(ingested_at) 
            FROM pipeline_logs
            WHERE status in ('SUCCESS', 'RUNNING');
        """

        last_load_date = conn.execute(query).scalar()
        return last_load_date
    
def initialize_run_log(engine, run_id:str, config: dict):
    init_run_log(engine, run_id, config)
    last_source_update = get_last_source_updated_on_from_log(engine)
    last_load_date = get_last_ingest_date_from_log(engine)

    return (last_source_update, last_load_date)

def update_log(engine, run_id, **kwargs):
    """Update log"""

    status = kwargs.get('status')
    mode = kwargs.get('mode')
    source_updated_on = kwargs.get('source_updated_on')

    values = {
        'run_id' : run_id
    }

    if status and status.upper() in ["SUCCESS", 'FAILED']:
        values.update({
            'status' : kwargs['status'],
            'end_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
        })

    if mode: values.update({'mode' : mode})

    if source_updated_on: values.update({'source_updated_on' : source_updated_on})

    set_values = ", ".join(f"{k} = :{k}" for k in values.keys() if k != 'run_id')

    with engine.connect() as conn:
        query = text(f"""
            UPDATE pipeline_logs
            SET
                {set_values}
            WHERE run_id = :run_id
        """)

        conn.execute(query, values)
        