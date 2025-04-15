import os
import subprocess
import time
import logging
import polars as pl


def run_command(command: str, message_error: str) -> None:
    result = subprocess.run(command, shell=True)
    if not result.returncode == 0:
        logging.error(message_error)
        raise Exception


def concat_parquet_to_csv_gzip(
    part_tables_name: list, output_folder: str, conca_csv_name: str
) -> None:
    for table_name in part_tables_name:
        start = time.time()
        csv_file_path = [
            f"{output_folder}/{csv_name}"
            for csv_name in os.listdir(output_folder)
            if ".csv.gz" not in csv_name
            and ".csv" not in csv_name
            and table_name in csv_name
        ]

        try:
            lf = pl.scan_parquet(csv_file_path)

            csv_name_path = os.path.join(
                output_folder, conca_csv_name.format(table_name=table_name)
            )

            lf.sink_csv(csv_name_path)

        except Exception as error:
            logging.error(
                f"Failed to concatenate csv for table {table_name}: {error}",
                stack_info=True,
            )
            raise error

        command_to_compress = f"gzip --fast {csv_name_path}"
        run_command(command_to_compress, f"Failed to compress the csv: {csv_name_path}")

        command_to_delete_parquet = f"rm -fr {output_folder}/*{table_name}*.parquet"
        run_command(
            command_to_delete_parquet,
            f"Failed to delete all the csvs for the table: {table_name} ",
        )

        end = time.time()
        execution_time = round((end - start) / 60, 2)
        logging.info(
            f"The csv {csv_name_path.replace(f'{output_folder}/', '')} was compressed.Time: {execution_time}"
        )

    logging.info("All the part tables were concatenated")


def get_all_sql_files(path_sql: str, remove_file: str = "") -> dict:
    """
    Reads all SQL files in a directory and returns a dictionary with file names as keys and queries as values.
    """
    sqls_name = os.listdir(path_sql)
    if remove_file:
        sqls_name = sqls_name.remove(remove_file)
    sql_dict = {}

    for sql_file in sqls_name:
        path = os.path.join(path_sql, sql_file)
        with open(path, encoding="utf-8") as f:
            sql = f.read()
        sql_name = sql_file.replace(".sql", "")
        sql_dict[sql_name] = sql

    return sql_dict


def get_sql_files(path_sql: str, sql_name_list: list) -> dict:
    sql_dict = {}

    for sql_file in sql_name_list:
        path = os.path.join(path_sql, sql_file)
        with open(path, encoding="utf-8") as f:
            sql = f.read()
        sql_name = sql_file.replace(".sql", "")
        sql_dict[sql_name] = sql

    return sql_dict


def write_remove_empty_lines_in_txt(file_name: str) -> None:
    # first get all lines from file
    with open(file_name, "r") as f:
        lines = f.readlines()

    # remove spaces
    lines = [line.replace(" ", "") for line in lines]

    # finally, write lines in the file
    with open(file_name, "w") as f:
        f.writelines(lines)
