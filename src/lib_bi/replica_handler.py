import polars as pl
import psycopg2


class ReplicaHandler:
    def __init__(self, database_name: str, port: int):
        """Initializes the connection to the database."""
        self.user = "micasino-bi-cloud-sql@mi-casino.iam"
        self.host = "localhost"
        self.port = port
        self.dbname = database_name

        self.conn = psycopg2.connect(
            host=self.host,
            database=self.dbname,
            user=self.user,
            port=self.port,
        )

    def execute_query_to_pl_df(
        self, sql: str, execute_options: dict = None
    ) -> pl.DataFrame:
        """Executes an SQL query and returns the results as a polars df."""
        result = pl.read_database(sql, self.conn, execute_options=execute_options)
        return result
