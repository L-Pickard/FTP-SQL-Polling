from pyodbc import connect, Error
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import os
from db_logger import write_to_log


def check_database_status(server, db):
    user = os.environ.get("Warehouse_db_User")
    passw = os.environ.get("Warehouse_db_Password")

    try:
        engine_url = f"mssql+pyodbc://{user}:{passw}@{server}/{db}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(engine_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            return bool(result and result[0] == 1)

    except Error as e:

        print(
            f"Check to see if {db} on {server} was available. An error occurred, see below:"
        )

        print(f"PyODBC error: {e}")

        return False

    except SQLAlchemyError as e:

        print(
            f"Check to see if {db} on {server} was available. An error occurred, see below:"
        )

        print(f"SQLAlchemy error: {e}")

        return False

    except Exception as e:

        print(
            f"Check to see if {db} on {server} was available. An error occurred, see below:"
        )

        print(f"An unexpected error occurred: {e}")

        return False


def execute_sql_procedure(
    server: str,
    db: str,
    table: str,
    sql: str,
    action: str,
    script: str,
    rows: int = None,
    start: str = None,
    end: str = None,
) -> bool:

    if check_database_status(server, db) == False:

        print("Database is not available so procedure has not been executed.")

        return False

    try:
        conn = connect(
            "DRIVER={SQL Server};SERVER="
            + server
            + ";DATABASE="
            + db
            + ";Trust_Connection=yes;"
        )

        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()

        conn.close()

        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt="Sql procedure has been executed Sucessfully",
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="INFO",
        )

        return True

    except Error as e:

        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt=str(e),
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="ERROR",
        )

        return False

    except Exception as e:
        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt=str(e),
            rows_txt=rows,
            start_txt=start,
            end_txt=end,
            log_level="ERROR",
        )
        return False


def get_sql_dataframe(
    server: str,
    db: str,
    table: str,
    sql: str,
    action: str,
    script: str,
    rows: int = None,
    start: str = None,
    end: str = None,
) -> pd.DataFrame:

    try:
        DRIVER_NAME = "SQL SERVER"
        SERVER_NAME = server
        DATABASE_NAME = db

        connection_string = f"""
        DRIVER={{{DRIVER_NAME}}};
        SERVER={SERVER_NAME};
        DATABASE={DATABASE_NAME};
        Trust_Connection=yes;
        """

        params = quote_plus(connection_string)

        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        with engine.connect() as conn:
            df = pd.read_sql_query(sql, conn)

        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt="pandas dataframe has been sucessfuly created from sql statement.",
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="INFO",
        )
        return df

    except SQLAlchemyError as e:
        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt=str(e),
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="ERROR",
        )
        return None
    
    except Exception as e:
        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt=str(e),
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="ERROR",
        )
        return None


def write_df_to_sql_db(
    server: str,
    db: str,
    table: str,
    df: pd.DataFrame,
    dtype: dict,
    action: str,
    script: str,
    rows: int = None,
    start: str = None,
    end: str = None,
) -> bool:

    user = os.environ.get("Warehouse_db_User")
    passw = os.environ.get("Warehouse_db_Password")

    try:
        engine_url = (
            f"mssql+pyodbc://{user}:{passw}@"
            + server
            + "/"
            + db
            + "?driver=ODBC+Driver+17+for+SQL+Server"
        )

        engine = create_engine(engine_url, fast_executemany=True)
        df.to_sql(
            table, engine, index=False, if_exists="append", dtype=dtype, chunksize=20000
        )

        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt="the pandas dataframe has been sucessfully written to the database table.",
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="INFO",
        )
        return True

    except SQLAlchemyError as e:
        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt=str(e),
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="ERROR",
        )
        return False
    
    except Exception as e:
        write_to_log(
            script_txt=script,
            table_txt=table,
            action_txt=action,
            message_txt=str(e),
            rows_count=rows,
            start_txt=start,
            end_txt=end,
            log_level="ERROR",
        )
        return False
