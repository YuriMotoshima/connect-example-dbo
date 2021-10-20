import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
import cx_Oracle
from cx_Oracle import Connection
from cx_Oracle import Cursor

load_dotenv(dotenv_path=fr"{os.getcwd()}\.env")

class SQLException(Exception):
    pass


class dbo_sql_oracle:
    def __init__(self, server:str = None, database:str = None, user:str = None, password:str = None) -> Connection:
        self.server = server or os.getenv('SERVER_ORACLE')
        self.database = database or os.getenv('DATABASE_ORACLE')
        self.user = user or os.getenv('USER_DB_ORACLE')
        self.password = password or os.getenv('PASSWORD_DB_ORACLE')
        self.conn = self._connector()


    def _connector(self) -> Connection:
        conn = f'{quote_plus(self.user)}/{quote_plus(self.password)}@{self.server}/{self.database}'
        return cx_Oracle.connect(conn)
    
    
    def _create_file_backup(self, name_table: str) -> None:
        cursor = self.cursor()
        table_bkp = cursor.execute(f"SELECT * FROM {name_table}")
        bkp = open(f"{name_table}.txt", "w+", encoding="utf-8")
        bkp.writelines(str(table_bkp.fetchall()))
        bkp.close()
        cursor.close()
    
    
    def cursor_conn(self) -> Cursor:
        csr = self.conn.cursor()
        return csr
