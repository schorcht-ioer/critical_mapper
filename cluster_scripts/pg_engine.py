from sqlalchemy import create_engine, text
import config, sql_conf

def run_sql(sql_statement):

    pg_engine = create_engine(config.CLUSTER_DATABASE_URI,encoding='latin1', echo=False)
    with pg_engine.begin() as conn:
        conn.execute(sql_conf.sql_proccesing_conf)
        conn.execute(text(sql_statement))
    conn.close()    
    pg_engine.dispose()    
    
def get_sql(sql_statement):
    result = None
    pg_engine = create_engine(config.CLUSTER_DATABASE_URI,encoding='latin1', echo=False)
    with pg_engine.begin() as conn:
        conn.execute(sql_conf.sql_proccesing_conf)
        result = conn.execute(text(sql_statement)).fetchall()
    conn.close()    
    pg_engine.dispose()
    return result

def vacuum(table):
    sql_statement = "VACUUM FULL " + table + ";"
    pg_engine = create_engine(config.CLUSTER_DATABASE_URI,encoding='latin1', echo=False)

    with pg_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        with conn.begin():
            conn.execute(text(sql_statement))

    conn.close()    
    pg_engine.dispose() 