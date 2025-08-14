import logging
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

logger = logging.getLogger(__name__)

class DBSnowflake:
    def __init__(self, account, user, password, database, schema, role, warehouse):
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
    
    def __load_query(file):
        try:    
            with open(file, 'r') as f:
                return f.read()
        except Exception as e:
            raise Exception(f"Fail to load query: {e}")
    
    def execute(self, sql: str, values: dict):
        query = text(self.__load_query(sql))
        values = values or {}

        with self.engine.begin() as conn:
            conn.execute(f'USE DATABASE "{self.database}"')
            conn.execute(f'USE WAREHOUSE "{self.warehouse}"')
            conn.execute(f'USE ROLE "{self.role}"')

            result = conn.execute(query, values) 
            return result
    

                