from flask_wtf import FlaskForm
from wtforms import BooleanField, FileField, PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length, ValidationError

from src.models import User


class RegisterForm(FlaskForm):
    def validate_username(
        self, username_to_check
    ):  # FlaskForm will search for all functions that start with validate_name, then check fields with that name
        user = User.query.filter_by(username=username_to_check.data).first()
        if user:
            raise ValidationError(
                "Username already exists! Please try a different username"
            )

    account = StringField(
        label="Account:", validators=[Length(min=2, max=60), DataRequired()]
    )
    username = StringField(
        label="User Name:", validators=[Length(min=2, max=30), DataRequired()]
    )
    password1 = PasswordField(
        label="Password:", validators=[Length(min=6), DataRequired()]
    )
    password2 = PasswordField(
        label="Confirm Password:", validators=[EqualTo("password1"), DataRequired()]
    )
    submit = SubmitField(label="Create Account")


class LoginForm(FlaskForm):
    account = StringField(label="Account:", validators=[DataRequired()])
    username = StringField(label="User Name:", validators=[DataRequired()])
    password = PasswordField(label="Password:", validators=[DataRequired()])
    submit = SubmitField(label="Sign in")


class UploadForm(FlaskForm):
    role = StringField(
        label="Role:", default="ACCOUNTADMIN", validators=[DataRequired()]
    )
    database = StringField(label="Database:", validators=[DataRequired()])
    warehouse = StringField(
        label="Warehouse:", default="COMPUTE_WH", validators=[DataRequired()]
    )
    schema = StringField(label="Schema:", default="public", validators=[DataRequired()])
    table = StringField(label="Table:", validators=[DataRequired()])
    create_table = BooleanField("Create Table:")
    filename = FileField(label="Data File:")
    submit = SubmitField(label="Upload Data")
