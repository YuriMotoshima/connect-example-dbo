from libs.connect_sql_server import dbo_sql_server
import pandas as pd

if __name__ == "__main__":
    
    sql = dbo_sql_server()
    df = pd.read_sql_query(sql="SELECT * FROM PPT.TB_SSP_DASHBOARD", con=sql.conn)
    
    session = sql.session()
    result = session.execute("SELECT * FROM PPT.TB_SSP_DASHBOARD")
    df2 = result.fetchall()    
    
    print("")