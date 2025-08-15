import logging
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine.base import Engine
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

logger = logging.getLogger(__name__)

class SnowflakeExecutor:

    def __init__(self, account: str, user: str, password: str, database: str, schema: str, role: str, warehouse: str, template_path: str):
        self.engine: Engine = create_engine(
                URL(
                account = account,
                user= user,
                password = password,
                database = database,
                schema = schema,
                role = role,
                warehouse = warehouse
            )
        )

        self.database = database
        self.warehouse = warehouse
        self.schema = schema
        self.role = role

        # sql template path
        self.template_path = template_path

        # Set context for database, warehouse and role
        @event.listens_for(self.engine, "connect")
        def set_snowflake_context(conn, _):
            cursor = conn.cursor()
            cursor.execute(f'USE DATABASE "{self.database}"')
            cursor.execute(f'USE WAREHOUSE "{self.warehouse}"')
            cursor.execute(f'USE ROLE "{self.role}"')
            cursor.close()

        
    def __load_query(self, sql: str):
        try:    
            file_path = Path(self.template_path) / sql
            with open(file_path, 'r') as f:
                return f.read()
        except Exception as e:
            raise Exception(f"Fail to load query: {e}")
        
    def __set_run_log(self, run_id: str, config: dict) -> None:
        """ Initialize run in logs"""
        
        values = {
            'run_id' : run_id,
            'ingested_at' : datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'start_time' : datetime.now(timezone.utc).strftime("%H:%M:%S"),
            'status' : 'RUNNING',
            'config' : str(config)
        }

        val_clause = ",".join([f':{k}' for k in values.keys()])

        with self.engine.begin() as conn:
            query = text(f"""
                INSERT INTO logs(
                    run_id,
                    ingested_at,
                    start_time,
                    status,
                    config
                )
                VALUES ({val_clause})
            """)

            conn.execute(query, values)
    
    def __get_last_source_updated_on(self):
        """ Fetch latest date the source was updated"""

        with self.engine.begin() as conn:
            query = """SELECT MAX(source_updated_on) FROM logs"""
            last_source_update = conn.execute(query).scalar()
            return last_source_update  
        
    def __get_last_ingest_date_from_log(self):
        """ Fetch the last load date"""

        with self.engine.begin() as conn:
            query = """
                SELECT MAX(ingested_at) 
                FROM logs
                WHERE status in ('SUCCESS', 'RUNNING');
            """

            last_load_date = conn.execute(query).scalar()
            return last_load_date
        
    def init_log(self, run_id: str, config: dict):
        self.__set_run_log(run_id, config)
        last_source_update = self.__get_last_source_updated_on()
        last_load_date = self.__get_last_ingest_date_from_log()

        return (last_source_update, last_load_date)

    def update_log(self, run_id:str, status : str = None, mode: str = None, source_updated_on: datetime = None ):
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

        if source_updated_on:
            values.update({'source_updated_on' : source_updated_on})
        
        set_values = ", ".join(f"{k} = :{k}" for k in values.keys() if k != 'run_id')

        with self.engine.connect() as conn:
            query = text(f"""
                UPDATE logs
                SET
                    {set_values}
                WHERE run_id = :run_id
            """)

            conn.execute(query, values)
    
    def create_table(self, sql: str):
        """ Create Table """

        query = self.__load_query(sql)
        table_name = sql.split(".")[0].split("_")[-1]
        logger.info(f"Creating Table '{table_name}'")
        with self.engine.begin() as conn:
            conn.execute(query)
    
    def get_tables(self) -> list:
        """ Fetch existing tables from database"""
        with self.engine.begin() as conn:
            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'PUBLIC'
                AND table_type = 'BASE TABLE';
            """
            result = conn.execute(query).fetchall()
            result = [r[0].lower() for r in result]
            return result
            
        
    def get_last_source_update(self):
        """ Fetch last 'source_updated_on' from Table 'crime' """
        with self.engine.begin() as conn:
            query = """SELECT MAX(source_updated_on) FROM crime"""
            result = conn.execute(query)
            return result.scalar()
    
    def load_crime_data(self, batchsize: int, df: pd.DataFrame):
        """ Performs batch insert to Table 'crime' """

        staging_table = "stg_crime"
        for start in range(0, len(df), batchsize):
            logger.info(f"Insert batch at idx: {start} - {start+batchsize}")
            batch = df.iloc[start : start + batchsize]

            # Creates a staging table
            batch.to_sql(staging_table, con=self.engine, schema=self.schema, if_exists='replace', index=False)

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
        
        # Clear staging table
        with self.engine.begin() as conn:
            query = f"""DROP TABLE IF EXISTS {staging_table}"""
            conn.execute(query)    

                