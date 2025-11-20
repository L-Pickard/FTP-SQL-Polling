from pyodbc import connect, Error
import time
import logging
import os

class LogDBHandler(logging.Handler):

    db_tbl_log = "[Database Log V2]"
    log_error_level = "DEBUG"

    def __init__(self, sql_conn, sql_cursor):
        logging.Handler.__init__(self)
        self.sql_cursor = sql_cursor
        self.sql_conn = sql_conn

    def emit(self, record):
        tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))

        self.log_msg = record.msg.strip().replace("'", "''")

        row_count = "NULL" if record.row_count is None else str(record.row_count)
        start_date = "NULL" if record.start_date is None else f"'{record.start_date}'"
        end_date = "NULL" if record.end_date is None else f"'{record.end_date}'"

        sql = f"""
        EXEC [dbo].[Insert Record Into Database Log]
            @Timestamp = '{tm}'
           ,@Level = '{str(record.levelname)}'
           ,@File_Name = '{str(record.filename)}'
           ,@Table = '{str(record.table)}'
           ,@Action = '{str(record.action)}'
           ,@Row_Count = {row_count}
           ,@Start_Date = {start_date}
           ,@End_Date = {end_date}
           ,@Message = '{self.log_msg}';
        """

        try:
            self.sql_cursor.execute(sql)
            self.sql_conn.commit()
        except Error as e:
            print(sql)
            print("CRITICAL DB ERROR! Logging to database not possible!")


try:
    user = os.environ.get("Warehouse_db_User")
    passw = os.environ.get("Warehouse_db_Password")
    server = "WHServer"
    db = "Warehouse"

    sql_conn = connect(
        "DRIVER={SQL Server};SERVER="
        + server
        + ";DATABASE="
        + db
        + ";UID="
        + user
        + ";PWD="
        + passw
    )
    sql_cursor = sql_conn.cursor()

except Error as e:
    print(f"Error connecting to the database: {e}")
    exit()
    
    
# Below is global initialization of database logger
    
debug_logger = logging.getLogger("db_log")
debug_logger.setLevel(logging.DEBUG)

if not debug_logger.handlers:
    db_handler = LogDBHandler(sql_conn, sql_cursor)
    db_handler.setLevel(logging.DEBUG)

    log_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d~%(levelname)s~%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db_handler.setFormatter(log_formatter)

    debug_logger.addHandler(db_handler)

def write_to_log(
    script_txt: str,
    table_txt: str,
    action_txt: str,
    message_txt: str,
    rows_count: int = None,
    start_txt: str = None,
    end_txt: str = None,
    log_level: str = "INFO",
) -> None:
    global debug_logger

    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if log_level.upper() not in log_levels:
        debug_logger.warning(f"Invalid log level: {log_level}. Defaulting to INFO.")
        level = logging.INFO
    else:
        level = log_levels[log_level.upper()]

    try:
        record = logging.LogRecord(
            name=debug_logger.name,
            level=level,
            pathname=script_txt,
            lineno=0,
            msg=message_txt,
            args=None,
            exc_info=None,
        )

        record.filename = script_txt
        record.table = table_txt
        record.action = action_txt
        record.row_count = rows_count
        record.start_date = start_txt
        record.end_date = end_txt

        debug_logger.handlers[0].emit(record)
        
    except Error as e:
        
        print(f"Failed to log to the database: {e}")
