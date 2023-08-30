import os
from datetime import timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from flask_bcrypt import Bcrypt
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect

from src.config import DEV_DB, PROD_DB, UPLOAD_FOLDER
from src.utils import delete_old_files

app = Flask(__name__)  # built-in variable that refers to the local python file

if os.environ.get("DEBUG") == "1":
    app.config["SQLALCHEMY_DATABASE_URI"] = DEV_DB
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = PROD_DB

app.config["SECRET_KEY"] = "350d448c9c69285b4bdf8529"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=1)
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
csrf = CSRFProtect(app)
login_manager = LoginManager(app)
login_manager.login_view = (
    "login_page"  # Redirect login_required decorator to the correct login page route
)
login_manager.login_message_category = "info"  # Customizes the flashed messages to info

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Create scheduler to delete old files from upload folder
scheduler = BackgroundScheduler()
scheduler.add_job(
    delete_old_files,
    "cron",
    day="*",
    hour="0",
    minute="0",
    args=[app.config["UPLOAD_FOLDER"]],
)

with app.app_context():
    db.create_all()

from src import routes
