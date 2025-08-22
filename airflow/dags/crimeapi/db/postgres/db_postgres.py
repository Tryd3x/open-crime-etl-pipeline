import logging
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

class PostgresExecutor:

    def __init__(self, host: str, port: str, username: str, password: str, db: str, template_path: str, schema: str = None):
        self.engine: Engine = create_engine(
            URL.create(
                drivername='postgresql',
                host=host,
                port=port,
                username=username,
                password=password,
                database=db
            )
        )

        self.schema = schema or 'public'

        #  Set the template path in the config
        self.template_path = template_path

    def __load_query(self, sql: str):
        try:
            file_path = Path(self.template_path) / sql
            with open(file_path, 'r') as f:
                return f.read()
        except Exception as e:
            raise Exception(f"Fail to load query: {e}")    
    
    def __set_run_log(self, run_id: str, config: dict) -> None:
        """ Initialize run in logs"""

        # Need to take in new params:
        # processed_at -> Tracks the date the load was performed on
        # load_date -> The date the load was scheduled for
        
        values = {
            'run_id' : run_id,
            'type' : 'SCHEDULED',
            'load_date' : datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'start_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
            'status' : 'RUNNING',
            'config' : str(config)
        }

        set_clause = ",".join(values.keys())
        val_clause = ",".join([f':{k}' for k in values.keys()])

        with self.engine.begin() as conn:
            query = text(f"""
                INSERT INTO logs({set_clause})
                VALUES ({val_clause})
            """)

            conn.execute(query, values)
    
    def __get_last_source_updated_on(self):
        """ Fetch latest date the source was updated"""

        with self.engine.begin() as conn:
            query = """SELECT MAX(source_updated_on) FROM crime"""
            last_source_update = conn.execute(query).scalar()
            return last_source_update  
        
    def __get_last_ingest_date_from_log(self):
        """ Fetch the last load date"""

        with self.engine.begin() as conn:
            query = """
                SELECT MAX(load_date) 
                FROM logs
                WHERE status in ('SUCCESS', 'RUNNING');
            """

            last_load_date = conn.execute(query).scalar()
            return last_load_date  
    
    def create_table(self, sql: str):
        """Create Table"""

        query = self.__load_query(sql)
        table_name = sql.split(".")[0].split("_")[-1]
        logger.info(f"Creating Table '{table_name}'")
        with self.engine.begin() as conn:
            conn.execute(query)
    
    def get_tables(self) -> list:
        with self.engine.begin() as conn:
            query = """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
            """
            result = conn.execute(query).fetchall()
            result = [r[0].lower() for r in result]
            return result

    def insert(self, table, **params):
        """ Generic Table insert """
        values = {k : v for k,v in params.items()}

        set_clause = ", ".join(params.keys())
        val_clause = ", ".join([f':{k}' for k in values.keys()])

        with self.engine.begin() as conn:
            query = text(f"""
                INSERT INTO {table}({set_clause})
                VALUES ({val_clause})
            """)

            conn.execute(query, values)
    
    def update(self, table, where: list[str], **params):
        """ Generic Table update"""
        values = {k : v for k,v in params.items()}

        set_values = ", ".join([f"{k} = :{k}" for k in values.keys() if k not in where])

        # Check if primary keys are passed
        missing_keys = [k for k in where if k not in values]
        if missing_keys:
            raise ValueError(f"Missing primary key(s) in update: {missing_keys}")

        where_clause = ' AND '.join([f"{k} = :{k}" for k in values.keys() if k in where])

        # whatever is in WHERE that shouldnt be in SET
        with self.engine.begin() as conn:
            query = text(f"""
                UPDATE {table}
                SET {set_values}
                WHERE {where_clause}
            """)

            conn.execute(query, values)
        
    def get_load_date_from_logs(self):
        """Fetch load date from Table 'logs'"""
        with self.engine.begin() as conn:
            query = "SELECT load_date FROM logs WHERE status = 'SUCCESS'"
            results = conn.execute(query).fetchall()
            return results

    def init_log(self, run_id: str, config: dict):
        self.__set_run_log(run_id, config)
        last_source_update = self.__get_last_source_updated_on()
        last_load_date = self.__get_last_ingest_date_from_log()

        return (last_source_update, last_load_date)        
    
    # def insert_log(self, run_id, load_date = None, **params):
    #     # What params are mandatory
    #     # run_id, load_date

    #     values = {
    #         'run_id' : run_id,
    #         'load_date' : datetime.now(timezone.utc).strftime('%Y-%m-%d'),
    #         'start_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
    #         'status' : 'RUNNING',
    #     }

    #     # Filter by params
    #     if load_date:
    #         # Update if performing a recovery
    #         values.update({'load_date': load_date})

    #     if params.get('type'):
    #         # SCHEDULED or RECOVERY
    #         values.update({'type': params.get('type')})

    #     if params.get('mode'):
    #         # INCREMENTAL OR FULL
    #         values.update({'mode': params.get('mode')})

    #     if params.get('end_time'):
    #         values.update({'end_time': params.get('end_time')})

    #     if params.get('source_updated_on'):
    #         # Most recent date from crime data
    #         values.update({'source_updated_on': params.get('source_updated_on')})

    #     if params.get('status'):
    #         # SUCCESS, RUNNING, FAILURE
    #         values.update({'status': params.get('status')})

    #     if params.get('config'):
    #         # Additional
    #         values.update({'config': params.get('config')})

    #     set_clause = ", ".join(values.keys())
    #     val_clause = ", ".join([f':{k}' for k in values.keys()])

    #     with self.engine.begin() as conn:
    #         query = text(f"""
    #             INSERT INTO logs({set_clause})
    #             VALUES ({val_clause})
    #         """)

    #         conn.execute(query)
    
    # This is not flexible and only updates on run_id, what if there were more than one primary key or where clause
    def update_log(self, run_id: str, load_date: str = None, type: str = None, status : str = None, mode: str = None, source_updated_on: datetime = None):
        values = {
            'run_id' : run_id
        }

        if status and status.upper() in ["SUCCESS", "FAILED"]:
            values.update({
                'status' : status,
                'end_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
                }
            )

        if mode:
            values.update({'mode' : mode})
        
        if type:
            values.update({'type' : type})

        if load_date:
            # Convert date to datetime then update values
            values.update({'load_date' : datetime.strptime(load_date, "%Y-%m-%d")})

        if source_updated_on:
            values.update({'source_updated_on' : source_updated_on})
        
        set_values = ", ".join([f"{k} = :{k}" for k in values.keys() if k != 'run_id'])

        with self.engine.begin() as conn:
            query = text(f"""
                UPDATE logs
                SET
                    {set_values}
                WHERE run_id = :run_id
            """)

            conn.execute(query, values)
    
    def load_crime_data(self, batchsize: int, df: pd.DataFrame):
        """ Performs batch insert to Table 'crime' """

        staging_table = "stg_crime"

        # Create the staging table
        with self.engine.begin() as conn:
            query = self.__load_query("create_stg_crime.sql")
            conn.execute(query)

        # Batch Insert
        for start in range(0, len(df), batchsize):
            logger.info(f"Insert batch at idx: {start} - {start+batchsize}")
            batch = df.iloc[start : start + batchsize]

            # Creates a staging table
            batch.to_sql(staging_table, con=self.engine, schema= self.schema,if_exists='append', index=False)

            # Dynamically create the clause
            columns = batch.columns.to_list()
            set_clause = ", ".join([f"{c} = c2.{c}" for c in columns if c != 'crime_id'])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"c2.{c}" for c in columns])

            with self.engine.begin() as conn:
                query = text(f"""
                    MERGE INTO crime c1
                    USING stg_crime c2
                    ON c1.crime_id = c2.crime_id
                    WHEN MATCHED THEN
                        UPDATE SET {set_clause} 
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values})
                """)

                conn.execute(query)
            
            with self.engine.begin() as conn:
                conn.execute("TRUNCATE TABLE stg_crime")

        
        # Clear staging table
        with self.engine.begin() as conn:
            query = f"""DROP TABLE IF EXISTS {staging_table}"""
            conn.execute(query)
        

        

