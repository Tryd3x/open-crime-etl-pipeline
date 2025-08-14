import logging 
from datetime import datetime, timezone, date
from sqlalchemy import MetaData, insert, update, select, func, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.dialects.postgresql import insert as upsert
import pandas as pd

logger = logging.getLogger(__name__)

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
    """ Function to perform batch insert into DB
    """
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

def get_last_source_update(engine: Engine) -> date:
    meta = MetaData()
    meta.reflect(engine)

    crime_table = meta.tables['crime']
    with engine.begin() as conn:
        st = select(func.max(crime_table.c.source_updated_on))
        result = conn.execute(st)
        last_update = result.scalar()
        return last_update