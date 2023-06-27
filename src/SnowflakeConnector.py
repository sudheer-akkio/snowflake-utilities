# Setup
import os
import time

import pandas as pd
import snowflake.connector as snow
from snowflake.connector import ProgrammingError
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry


class SnowflakeConnector:
    # Constructor
    def __init__(self, account, username, password) -> None:
        self.account = account
        self.username = username
        self.password = password

        self.role = "ACCOUNTADMIN"
        self.database = ""
        self.warehouse = "COMPUTE_WH"
        self.schema = "public"
        self.table = ""
        self.data = pd.DataFrame()
        self.filename = ""

        self._table_check = False

    # Public Methods
    def setup(self):
        # Initialize connection
        self.__create_connection()

        # Set role
        self.__set_role()

        # Check valid compute warehouse is selected
        self.__check_warehouse()

        # Check valid database is selected
        self.__check_database()

        # Create schema
        self.__create_schema()

    def import_data(self):
        if self.filename != "":
            print("Importing data...")

            self.data = pd.read_csv(self.filename, index_col=False)
            self.data.columns = map(lambda x: str(x).upper(), self.data.columns)
        else:
            raise ValueError("Filename is empty.")

        print("Completed!\n")

    def create_table(self):
        if (
            not self.data.empty
            and self.database != ""
            and self.table != ""
            and self.schema != ""
        ):
            print("Creating table...")
            # Setup sqlalchemy engine
            conn_string = f"snowflake://{self.username}:{self.password}@{self.account}/{self.database.lower()}/{self.schema.lower()}"
            engine = create_engine(conn_string)

            if_exists = "replace"

            self.data.to_sql(
                self.table.lower(),
                con=engine,
                if_exists=if_exists,
                index=False,
                index_label=None,
                method=pd_writer,
            )

            print("Completed!\n")

    # Private Methods
    def __create_connection(self) -> None:
        # Setup connection object
        self.conn = snow.connect(
            account=self.account, user=self.username, password=self.password
        )
        self.curr = self.conn.cursor()

    def __set_role(self) -> None:
        # Set role
        valid_role_list = ["SYSADMIN", "ACCOUNTADMIN"]
        if self.role.upper() not in valid_role_list:
            raise ValueError("Role must be either sysadmin or accountadmin")

        sql_cmd = "USE ROLE " + self.role.upper()
        self.__execute_query(sql_cmd)

    def __check_warehouse(self):
        # Check to see if warehouse already exists
        sql_command = "SHOW WAREHOUSES LIKE '" + self.warehouse + "%'"
        result = self.__execute_query(sql_command).fetchall()
        warehouse_list = [wh[0] for wh in result]

        if self.warehouse not in warehouse_list:
            raise ValueError(
                self.warehouse
                + " does not exist in current compute warehouses. Please specify a valid compute warehouse."
            )

        sql_command = "USE WAREHOUSE " + self.warehouse.upper()
        self.__execute_query(sql_command)

    def __check_database(self):
        # Check to see if database already exists
        sql_command = "SHOW DATABASES LIKE '" + self.database + "%'"
        result = self.__execute_query(sql_command).fetchall()
        db_list = [db[1] for db in result]

        if self.database not in db_list:
            raise ValueError(
                self.database
                + " does not exist in current databases. Please specify a valid database."
            )

        sql_command = "USE DATABASE " + self.database.upper()
        self.__execute_query(sql_command)

    def __create_schema(self):
        # Create schema if it doesn't exist

        sql_command = "CREATE SCHEMA IF NOT EXISTS " + self.schema
        self.__execute_query(sql_command)

        sql_command = "USE SCHEMA " + self.schema
        self.__execute_query(sql_command)

    # Create run query method to does error checking
    def __execute_query(self, sql_cmd):
        # Wait for the query to finish running and raise an error
        # if a problem occurred with the execution of the query.
        try:
            result = self.curr.execute(sql_cmd)
            query_id = self.curr.sfqid
            while self.conn.is_still_running(
                self.conn.get_query_status_throw_if_error(query_id)
            ):
                time.sleep(1)
        except ProgrammingError as err:
            print("Programming Error: {0}".format(err))

        return result

    # Destructor
    def __del__(self):
        print("Closing connection!\n")
        self.curr.close()
        self.conn.close()


if __name__ == "__main__":
    print("Starting process...\n")
