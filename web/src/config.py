import os

UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")
DEV_DB = "sqlite:///" + os.path.join(os.getcwd(), "src", "snowflake.db")

pg_user = "admin"
pg_pass = "admin"
pg_db = "snowflake"
pg_host = "db"  # postgres is going to sit on same production server as flask application, so want localhost
pg_port = 5432

PROD_DB = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
