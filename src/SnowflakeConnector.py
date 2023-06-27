# Setup
import os
import time

import pandas as pd
import snowflake.connector as snow
from snowflake.connector import ProgrammingError
from snowflake.connector.pandas_tools import write_pandas


class SnowflakeConnector:
    # Constructor
    def __init__(self, account, username, password) -> None:
        self.account = account
        self.username = username
        self.password = password

        self.role = "ACCOUNTADMIN"
        self.database = ""
        self.warehouse = "COMPUTE_WH"
        self.table = ""
        self.data = []
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

        # Check if table exists. If table does exists then append data to new table. If it doesn't then attempt to auto create the table using write_pandas.
        # Return a boolean flag if table exists or not
        # SNN: Scratch that will need to use to_sql in sqlalchemy
        self._table_check = self.__check_table()

    def import_data(self):
        if self.filename != "":
            self.data = pd.read_csv(self.filename, index_col=None)
        else:
            raise ValueError("Filename is empty.")

    def write_data(self):
        pass
        # if not self.data:
        #     if self._table_check:

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
        db_list = [db[0] for db in result]

        if self.database not in db_list:
            raise ValueError(
                self.database
                + " does not exist in current databases. Please specify a valid database."
            )

        sql_command = "USE DATABASE " + self.database.upper()
        self.__execute_query(sql_command)

    def __check_table(self):
        # Check to see if table already exists
        sql_command = "SHOW TABLES LIKE '" + self.table + "%'"
        result = self.__execute_query(sql_command).fetchall()
        tb_list = [tb[0] for tb in result]

        check = True

        if self.table not in tb_list:
            print(
                self.table
                + " does not exist in current database tables. This table will try to be auto generated when writing data."
            )
            check = False

        return check

    # Create run query method to does error checking
    def __execute_query(self, sql_cmd):
        # Wait for the query to finish running and raise an error
        # if a problem occurred with the execution of the query.
        try:
            self.curr.execute(sql_cmd)
            query_id = self.curr.sfqid
            while self.conn.is_still_running(
                self.conn.get_query_status_throw_if_error(query_id)
            ):
                time.sleep(1)
        except ProgrammingError as err:
            print("Programming Error: {0}".format(err))

    # Destructor
    def __del__(self):
        print("Closing connection!")
        self.curr.close()
        self.conn.close()


if __name__ == "__main__":
    print("Success!")


account = "pnb55073.us-east-1"
user = "sudheer"
password = os.getenv("SNOWSQL_PWD")
database = ""
table = ""
filename = ""

obj = SnowflakeConnector(account, user, password)

obj.setup()

obj.filename = filename

obj.import_data()

obj.write_data()
