import json
import os

from flask import (
    flash,
    get_flashed_messages,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required, login_user, logout_user
from werkzeug.utils import secure_filename

from src import app, db
from src.forms import LoginForm, RegisterForm, UploadForm
from src.models import User
from src.SnowflakeConnector import (
    SnowflakeConnector,
    SnowflakeConnectorEncoder,
    snowflake_connector_decoder,
    test_snowflake_connection,
)


@app.route("/")  # Decorator -- what URL should I navigate and display the HTML code?
@app.route("/home")  # This is how we can handle multiple routes for the same request
def home_page():
    return render_template("home.html")


@app.route("/upload", methods=["GET", "POST"])
def upload_page():
    form = UploadForm()

    if current_user.is_authenticated:
        if form.validate_on_submit():
            # Retrieve the serialized object from the session
            serialized_data = session.get("snowflake_obj", None)

            if serialized_data:
                try:
                    # Deserialize the object
                    obj = json.loads(
                        serialized_data, object_hook=snowflake_connector_decoder
                    )

                    obj.role = form.role.data
                    obj.database = form.database.data
                    obj.warehouse = form.warehouse.data
                    obj.schema = form.schema.data
                    obj.table = form.table.data

                    # Setup connection, warehouse, role, database, and schema
                    obj.setup()

                    data_file = request.files["filename"]
                    if data_file:
                        filename = secure_filename(data_file.filename)
                        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)

                        # Should I copy the file to local storage if this is running on the server?
                        # Save the file path in the form field
                        if not os.path.exists(app.config["UPLOAD_FOLDER"]):
                            os.makedirs(app.config["UPLOAD_FOLDER"])

                        # Open the file in write mode and write some content
                        data_file.save(file_path)

                        form.filename.data = file_path
                        obj.filename = file_path

                        obj.import_data()

                        # Create empty table with table definition based on input dataframe
                        if form.create_table.data:
                            obj.create_table()
                            flash(
                                f"Created {obj.table} table successfully!",
                                category="success",
                            )

                        # Add all input data into the table
                        obj.add_rows()

                        flash("Data uploaded successfully!", category="success")

                    else:
                        flash(
                            "Invalid data file.",
                            category="danger",
                        )

                except Exception as err:
                    flash(f"Error in file upload: {str(err)}", category="danger")

            else:
                flash(
                    "No snowflake object found in the session. Please login first.",
                    category="danger",
                )
    else:
        flash("Please login first.", category="danger")

        return redirect(url_for("login_page"))

    if form.errors != {}:
        for err_msg in form.errors.values():
            flash(
                f"There was an error with creating a user: {err_msg}", category="danger"
            )

    return render_template("upload.html", form=form)


@app.route("/register", methods=["GET", "POST"])
def register_page():
    form = RegisterForm()
    if (
        form.validate_on_submit()
    ):  # validate_on_submit will automatically run the validation specified in RegisterForm class and only create user if all validations are successful
        user_to_create = User(
            account=form.account.data,
            username=form.username.data,
            password=form.password1.data,
        )

        db.session.add(user_to_create)
        db.session.commit()

        login_user(user_to_create)
        flash(
            f"Account created successfully! You are now logged in as {user_to_create.username}",
            category="success",
        )

        return redirect(
            url_for("upload_page")
        )  # make sure to import these methods from flask

    if form.errors != {}:
        for err_msg in form.errors.values():
            flash(
                f"There was an error with creating a user: {err_msg}", category="danger"
            )

    return render_template("register.html", form=form)


@app.route("/login", methods=["GET", "POST"])
def login_page():
    form = LoginForm()

    if form.validate_on_submit():
        attempted_user = User.query.filter_by(username=form.username.data).first()
        if attempted_user and attempted_user.check_password_correction(
            attempted_password=form.password.data
        ):
            try:
                # Test connection will be successfull, or error (used to fail early)
                test_snowflake_connection(
                    form.account.data, form.username.data, form.password.data
                )

                # Create a SnowflakeConnector object and serialize it to JSON
                obj = SnowflakeConnector(
                    form.account.data, form.username.data, form.password.data
                )
                serialized_data = json.dumps(obj, cls=SnowflakeConnectorEncoder)

                # Store the serialized object in the session
                session["snowflake_obj"] = serialized_data

                login_user(attempted_user)
                flash(
                    f"Success! You are logged in as: {form.username.data}",
                    category="success",
                )

                return redirect(url_for("upload_page"))
            except ValueError as err:
                flash(
                    f"Error during login: {str(err)}",
                    category="danger",
                )
        else:
            flash(
                "Username and password are not match! Please try again",
                category="danger",
            )

    if form.errors != {}:
        for err_msg in form.errors.values():
            flash(f"There was an error with logging in: {err_msg}", category="danger")
    return render_template("login.html", form=form)


@app.route("/logout")
def logout_page():
    # Remove connection object from session, which will be a logout
    session.pop("snowflake_obj", None)
    logout_user()
    flash("You have been logged out!", category="info")
    return redirect(url_for("home_page"))
