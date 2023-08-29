import os

UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")
DEV_DB = "sqlite:///" + os.path.join(os.getcwd(), "src", "snowflake.db")
