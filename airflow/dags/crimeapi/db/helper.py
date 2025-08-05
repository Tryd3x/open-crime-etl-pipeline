import logging 
from datetime import datetime, timezone, date
from sqlalchemy import MetaData, insert, update, select, func
from sqlalchemy.engine.base import Engine
from sqlalchemy.dialects.postgresql import insert as upsert
import pandas as pd

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
        last_source_update = result.scalar()

        # Fetch last date of ingest
        st3 = select(func.max(log_table.c.ingested_at)).where(log_table.c.status.in_(['SUCCESS', 'RUNNING']))
        result = conn.execute(st3)
        last_load_date = result.scalar()
        return (run_id, last_source_update, last_load_date)
    
def update_run_log(engine: Engine, run_id: int, status : str = None, mode: str = None, source_updated_on: datetime = None) -> None:

    meta = MetaData()
    meta.reflect(engine)
    log_table = meta.tables['pipeline_logs']

    values = {}

    if status and status.upper() in ["SUCCESS", "FAILED"]:
        values.update({
            'status' : status,
            'end_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
            }
        )

    if mode:
        values.update({'mode' : mode})

    if source_updated_on:
        values.update({'source_updated_on' : source_updated_on})

    if not values:
        return
        
    with engine.begin() as conn:
        st = update(log_table).where(log_table.c.run_id == run_id).values(**values)
        conn.execute(st)

def load_to_postgres(engine: Engine, batchsize: int, table, df: pd.DataFrame):
    """ Function to perform batch insert into DB"""
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]

    for start in range(0, len(df), batchsize):
        logger.info(f"Insert batch at idx: {start} - {start+batchsize} ")
        batch = df.iloc[start : start + batchsize]
        with engine.begin() as conn:
            st = upsert(table).values(batch.to_dict(orient='records'))
            up_st = st.on_conflict_do_update(
                index_elements = [table.c.crime_id],
                set_= {c.key: c for c in st.excluded if c.key not in key_columns}
            )

            conn.execute(up_st)

# This is incorrect, must fetch the source date from the data table and not the meta table
def get_last_source_update(engine: Engine) -> date:
    meta = MetaData()
    meta.reflect(engine)

    crime_table = meta.tables['crime']
    with engine.begin() as conn:
        st = select(func.max(crime_table.c.source_updated_on))
        result = conn.execute(st)
        last_update = result.scalar()
        return last_update