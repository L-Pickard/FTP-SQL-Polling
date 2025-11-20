from ftplib import FTP
import os
import pandas as pd
from pyodbc import connect
from datetime import datetime
from SQL_Functions import execute_sql_procedure
from db_logger import write_to_log

# Environment variables for FTP
ftp_server = os.environ.get("FTP_SERVER")
ftp_username = os.environ.get("FTP_USER")
ftp_password = os.environ.get("FTP_PASS")
ftp_directory = "/"

script_name = os.path.basename(__file__)


def process_preorders(ftp):

    name_script = script_name

    path = "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/preorders.csv"
    path_xml = "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/upload preorders.xml"
    procedure = "EXECUTE [Update fPreorder];"
    flag_procedure = """
    UPDATE [LP_Toolbox].[dbo].[Procedure Flag]
    SET [Exec Flag] = 1
    WHERE [Procedure Name] = 'Preorder Customer Activity Alert';
    """

    if os.path.exists(path):
        os.remove(path)

    with open(path, "wb") as file:
        ftp.retrbinary("RETR preorders.csv", file.write)
        ftp.delete("preorders.csv")

    df = pd.read_csv(path)

    if os.path.exists(path_xml):
        os.remove(path_xml)

    df.to_xml(path_xml, index=False)

    execute_sql_procedure(
        server="WHServer",
        db="Warehouse",
        table="fPreorder",
        sql=procedure,
        action="Execute procedure to update fPreorder table.",
        script=name_script,
    )

    execute_sql_procedure(
        server="WHServer",
        db="LP_Toolbox",
        table="Procedure Flag",
        sql=flag_procedure,
        action="Execute procedure to update procedure flag table.",
        script=name_script,
    )

    trigger_data = [{"Command": "Refresh PB Dataset"}]
    df_trigger = pd.DataFrame(trigger_data)
    trigger_path = "C:/Users/leo.pickard/Desktop/Automated Projects/Refresh Files"

    df_trigger.to_csv(
        os.path.join(
            trigger_path,
            datetime.now().strftime("%d.%m.%y %H.%M.%S") + " Refresh Data.csv",
        ),
        index=False,
    )


def process_preorders_history(ftp):

    name_script = script_name

    path = "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/preorders-history.csv"
    path_xml = "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/upload preorders-history.xml"
    procedure = "EXECUTE [Update fPreorder History];"

    if os.path.exists(path):
        os.remove(path)

    with open(path, "wb") as file:
        ftp.retrbinary("RETR preorders-history.csv", file.write)
        ftp.delete("preorders-history.csv")

    df = pd.read_csv(path)

    if os.path.exists(path_xml):
        os.remove(path_xml)

    df.to_xml(path_xml, index=False)

    execute_sql_procedure(
        server="WHServer",
        db="Warehouse",
        table="fPreorder History",
        sql=procedure,
        action="Execute procedure to update fPreorder History table.",
        script=name_script
    )

    trigger_data = [{"Command": "Refresh PB Dataset"}]
    df_trigger = pd.DataFrame(trigger_data)
    trigger_path = "C:/Users/leo.pickard/Desktop/Automated Projects/Refresh Files"

    df_trigger.to_csv(
        os.path.join(
            trigger_path,
            datetime.now().strftime("%d.%m.%y %H.%M.%S") + " Refresh Data.csv",
        ),
        index=False,
    )


def parse_price(price_str):
    parts = price_str.split()

    currency = parts[0].split("=")[0].split("_")[1]

    whs = float(parts[0].split("=")[1])
    srp = float(parts[1].split("=")[1])

    return currency, whs, srp


def process_active_lines(ftp):

    name_script = script_name

    path = "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/active.csv"
    upload_path = (
        "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/Active Import.csv"
    )
    procedure = "EXECUTE [Update dPreorder Lines];"

    if os.path.exists(path):
        os.remove(path)

    with open(path, "wb") as file:
        ftp.retrbinary("RETR active.csv", file.write)
        ftp.delete("active.csv")

    df = pd.read_csv(path)
    
    df = df[~df["description"].isnull() & (df["description"].str.strip() != "")]

    df["Currency"], df["WHS"], df["SRP"] = zip(*df["price1"].apply(parse_price))

    if os.path.exists(upload_path):
        os.remove(upload_path)

    df.to_csv(upload_path, index=False, sep='~')

    execute_sql_procedure(
        server="WHServer",
        db="Warehouse",
        table="dPreorder Lines",
        sql=procedure,
        action="Execute procedure to update dPreorder Lines table.",
        script=name_script,
    )
    
def process_b2b_events(ftp):
    
    name_script = script_name
    
    path = "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/events.csv"
    upload_path = (
        "C:/Users/leo.pickard/Desktop/Misc Bulk Inserts/Preorder Data/events import.csv"
    )
    procedure = "EXECUTE [Update fB2B Events];"
    
    if os.path.exists(path):
        os.remove(path)

    with open(path, "wb") as file:
        ftp.retrbinary("RETR events.csv", file.write)
        ftp.delete("events.csv")

    df = pd.read_csv(path)
    
    if os.path.exists(upload_path):
        os.remove(upload_path)

    df.to_csv(upload_path, index=False, sep='~')

    execute_sql_procedure(
        server="WHServer",
        db="Warehouse",
        table="fB2B Events",
        sql=procedure,
        action="Execute procedure to insert new data into fB2B Events table.",
        script=name_script,
    )

def get_sql_file_list():
    try:
        with connect(
            "DRIVER={SQL Server};SERVER=WHServer;DATABASE=LP_Toolbox;Trust_Connection=yes;"
        ) as conn:
            with conn.cursor() as cursor:
                query = "SELECT [File Name] FROM [Owtanet FTP Files]"
                cursor.execute(query)
                rows = cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:

        write_to_log(
            script_txt=script_name,
            table_txt="Owtanet FTP Files",
            action_txt="Retrieving owtanet file list from database.",
            message_txt=f"An unexpected error occurred: {str(e)}",
            log_level="CRITICAL",
        )

        print(f"SQL execution error: {e}")

        return []


def insert_files_to_sql(file_list):

    name_script = script_name

    try:
        with connect(
            "DRIVER={SQL Server};SERVER=WHServer;DATABASE=LP_Toolbox;Trust_Connection=yes;"
        ) as conn:
            with conn.cursor() as cursor:
                for file_name in file_list:
                    cursor.execute(
                        "INSERT INTO [Owtanet FTP Files] ([File Name]) VALUES (?)",
                        file_name,
                    )
                conn.commit()
    except Exception as e:

        write_to_log(
            script_txt=name_script,
            table_txt="Owtanet FTP Files",
            action_txt="writing new owtanet file list to database.",
            message_txt=f"An unexpected error occurred: {str(e)}",
            log_level="CRITICAL",
        )

        print(f"SQL execution error: {e}")


def files_in_ftp_directory(ftp, directory):

    name_script = script_name

    try:
        ftp.cwd(directory)
        return ftp.nlst()
    except Exception as e:

        write_to_log(
            script_txt=name_script,
            table_txt="Owtanet FTP Server",
            action_txt="Retrieve list of files from owtanet ftp server.",
            message_txt=f"An unexpected error occurred: {str(e)}",
            log_level="CRITICAL",
        )

        print(f"Error listing FTP directory: {e}")

        return []


def list_comparison(original_list, new_list):
    return [x for x in new_list if x not in original_list]


def files_to_process(ftp):
    print("Fetching file list from FTP directory...")
    new_list = files_in_ftp_directory(ftp, ftp_directory)
    print(f"Files in FTP directory: {new_list}")

    print("Fetching file list from SQL database...")
    original_list = get_sql_file_list()
    print(f"Files in SQL database: {original_list}")

    diff = list_comparison(original_list, new_list)
    print(f"Files to process: {diff}")

    return diff


def main():
    name_script = script_name

    truncate_sql = "TRUNCATE TABLE [Owtanet FTP Files];"

    file_function_dict = {
        "preorders.csv": process_preorders,
        "preorders-history.csv": process_preorders_history,
        "active.csv": process_active_lines,
        "events.csv": process_b2b_events,
    }

    try:
        with FTP(ftp_server) as ftp:
            ftp.login(ftp_username, ftp_password)
            files = files_to_process(ftp)

            for file in files:
                if file in file_function_dict:
                    function_to_call = file_function_dict[file]
                    if function_to_call:
                        print(
                            f"Processing {file} with function {function_to_call.__name__}"
                        )
                        function_to_call(ftp)
            
            execute_sql_procedure(
                server="WHServer",
                db="LP_Toolbox",
                table="Owtanet FTP Files",
                sql=truncate_sql,
                action="Execute procedure to update Owtanet FTP Files table.",
                script=name_script,
            )

            new_list = files_in_ftp_directory(ftp, ftp_directory)
            insert_files_to_sql(new_list)

    except Exception as e:

        write_to_log(
            script_txt=name_script,
            table_txt="Owtanet FTP Files",
            action_txt="Execute script to update Owtanet FTP Files table. An error has occurred.",
            message_txt=f"An unexpected error occurred: {str(e)}",
            log_level="CRITICAL",
        )

        print(f"Error processing new files: {e}")


if __name__ == "__main__":
    main()
