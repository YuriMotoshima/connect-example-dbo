import os
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv(dotenv_path=fr"{os.getcwd()}\.env")

class SQLException(Exception):
    pass

class dbo_sql_server:
    def __init__(self, server:str = None, database:str = None, user:str = None, password:str = None) -> Engine:
        self.server = server or os.getenv('SERVER_SQL')
        self.database = database or os.getenv('DATABASE_SQL')
        self.user = user or os.getenv('USER_DB_SQL')
        self.password = password or os.getenv('PASSWORD_DB_SQL')
        self.conn = self._connector()
        
        
    def _connector(self) -> Engine:
        conn = f'mssql+pyodbc://{quote_plus(self.user)}:{quote_plus(self.password)}@{quote_plus(self.server)}/{quote_plus(self.database)}?driver=ODBC+Driver+17+for+SQL+Server'
        return create_engine(conn, echo=False, echo_pool='debug')
    
    
    def _create_file_backup(self, name_table: str):
        session = self.session()
        table_bkp = session.execute(f"SELECT * FROM {name_table}")
        bkp = open(f"{name_table}.txt", "w+", encoding="utf-8")
        bkp.writelines(str(table_bkp.fetchall()))
        bkp.close()
        session.close()
    
    
    def session(self) -> Session:
        session = Session(bind=self.conn)
        return session
    
    
    def reset_table(self, name_table : str, name_columns : str) -> None:
        self._create_file_backup(name_table=name_table)
        
        session = self.session()
        session.execute(f"DROP TABLE {name_table}")
        session.commit()
        
        created_table = f' CREATE TABLE {name_table} ( {" varchar(MAX), ".join(name_columns)} varchar(MAX) );'
        session.execute(created_table)
        session.commit()
        session.close()
        
        
    def update_table(self, name_table : str, name_columns : str, data : list) -> None:
        session = self.session()
        insert_into = f'INSERT INTO {name_table} ({", ".join(name_columns)})'
        queries = [f'{insert_into} VALUES ({str(", ".join(row))});' for row in data]
        
        for query in queries:
            session.execute(query)
            session.commit()
        
        session.close()
