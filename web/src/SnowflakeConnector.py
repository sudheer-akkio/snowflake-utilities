# Setup
import json
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

    # Public Methods
    def setup(self):
        # Initialize connection objects
        self._create_connection()

        # Set role
        self._set_role()

        # Check valid compute warehouse is selected
        self._check_warehouse()

        # Check valid database is selected
        self._check_database()

        # Create schema
        self._create_schema()

    def import_data(self):
        if self.filename != "":
            print("Importing data...")

            self.data = pd.read_csv(self.filename, index_col=None)
            self.data.columns = map(
                lambda x: str(x).upper().replace(" ", "_"), self.data.columns
            )

        else:
            raise ValueError("Filename is empty.")

        print("Completed!\n")

    def create_table(self):
        # Create empty table with appropriatea schema based on imported data
        if (
            not self.data.empty
            and self.database != ""
            and self._table != ""
            and self.schema != ""
        ):
            print("Creating table...")
            # Setup sqlalchemy engine
            conn_string = f"snowflake://{self.username}:{self.password}@{self.account}/{self.database.lower()}/{self.schema.lower()}"
            registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
            engine = create_engine(conn_string)

            # Raise a ValueError if table already exists
            if_exists = "fail"

            self.data.to_sql(
                self._table,
                con=engine,
                if_exists=if_exists,
                index=False,
                index_label=None,
                method=pd_writer,
            )

            print(f"Created {self._table} table succesfully!")

    def add_rows(self):
        # Add rows to existing table
        if self._check_table_exists():
            print("Adding rows...")

            conn_string = f"snowflake://{self.username}:{self.password}@{self.account}/{self.database.lower()}/{self.schema.lower()}"
            engine = create_engine(conn_string)

            # append data to existing table
            if_exists = "append"

            self.data.to_sql(
                self._table,
                con=engine,
                if_exists=if_exists,
                index=False,
                index_label=None,
                method=pd_writer,
            )

            print("Completed!\n")
        else:
            raise ValueError(
                self._table + " does not exist. Create table before adding rows."
            )

    def delete_table(self):
        # removes all rows from a table but leaves the table intact

        sql_command = "TRUNCATE TABLE IF EXISTS " + self._table
        self._execute_query(sql_command)

    def to_dict(self):
        return {
            "account": self.account,
            "username": self.username,
            "password": self.password,
            "role": self.role,
            "database": self.database,
            "warehouse": self.warehouse,
            "schema": self.schema,
            "table": self.table,
            "data": self.data.to_dict(orient="records"),
            "filename": self.filename,
        }

    @classmethod
    def from_dict(cls, data):
        instance = cls(
            data["account"],
            data["username"],
            data["password"],
        )
        instance.role = data["role"]
        instance.database = data["database"]
        instance.warehouse = data["warehouse"]
        instance.schema = data["schema"]
        instance.table = data["table"]
        instance.data = pd.DataFrame(data["data"])
        instance.filename = data["filename"]
        return instance

    # Private Methods
    def _create_connection(self) -> None:
        # Setup connection object
        self.conn = snow.connect(
            account=self.account, user=self.username, password=self.password
        )
        self.curr = self.conn.cursor()

    def _set_role(self) -> None:
        # Set role
        valid_role_list = ["SYSADMIN", "ACCOUNTADMIN"]
        if self.role.upper() not in valid_role_list:
            raise ValueError("Role must be either sysadmin or accountadmin")

        sql_cmd = "USE ROLE " + self.role.upper()
        self._execute_query(sql_cmd)

    def _check_warehouse(self):
        # Check to see if warehouse already exists
        sql_command = "SHOW WAREHOUSES LIKE '" + self.warehouse + "%'"
        result = self._execute_query(sql_command).fetchall()
        warehouse_list = [wh[0] for wh in result]

        if self.warehouse not in warehouse_list:
            raise ValueError(
                self.warehouse
                + " does not exist in current compute warehouses. Please specify a valid compute warehouse."
            )

        sql_command = "USE WAREHOUSE " + self.warehouse.upper()
        self._execute_query(sql_command)

    def _check_database(self):
        # Check to see if database already exists
        sql_command = "SHOW DATABASES LIKE '" + self.database + "%'"
        result = self._execute_query(sql_command).fetchall()
        db_list = [db[1] for db in result]

        if self.database.upper() not in db_list:
            raise ValueError(
                self.database
                + " does not exist in current databases. Please specify a valid database."
            )

        sql_command = "USE DATABASE " + self.database.upper()
        self._execute_query(sql_command)

    def _create_schema(self):
        # Create schema if it doesn't exist

        sql_command = "CREATE SCHEMA IF NOT EXISTS " + self.schema
        self._execute_query(sql_command)

        sql_command = "USE SCHEMA " + self.schema
        self._execute_query(sql_command)

    # Create run query method to does error checking
    def _execute_query(self, sql_cmd):
        # Wait for the query to finish running and raise an error
        # if a problem occurred with the execution of the query.
        try:
            result = self.curr.execute(sql_cmd)
            query_id = self.curr.sfqid
            while self.conn.is_still_running(
                self.conn.get_query_status_throw_if_error(query_id)
            ):
                time.sleep(1)

            return result

        except ProgrammingError as err:
            print("Programming Error: {0}".format(err))

    def _check_table_exists(self):
        # Check to see if table already exists
        sql_command = (
            "SHOW TABLES LIKE '"
            + self._table
            + "%' IN "
            + self.database
            + "."
            + self.schema
        )
        result = self._execute_query(sql_command).fetchall()
        table_list = [tb[1] for tb in result]

        return self._table.upper() in table_list

    # Accessors
    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, table):
        self._table = table.lower().replace("-", "_")

    # Destructor
    def __del__(self):
        print("Closing connection!\n")
        if hasattr(self, "curr"):
            self.curr.close()
        if hasattr(self, "conn"):
            self.conn.close()


class SnowflakeConnectorEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SnowflakeConnector):
            return obj.to_dict()
        return super().default(obj)


def snowflake_connector_decoder(data):
    keys = ["account", "username", "password"]

    if all(key in data for key in keys):
        return SnowflakeConnector.from_dict(data)
    return data


def test_snowflake_connection(account, username, password):
    try:
        # Attempt to establish a connection
        conn = snow.connect(account=account, user=username, password=password)

        # Close the connection
        conn.close()

        print("Connection successful!")
    except Exception as e:
        raise ValueError("Connection failed:", e)


if __name__ == "__main__":
    print("Starting process...\n")
