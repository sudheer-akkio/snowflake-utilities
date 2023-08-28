# Snowflake Utilities

This repository contains utilities for programmatically accessing and working with Snowflake.

## Installation and Setup

### Python Dependencies

You can get these as follows.

```shell
brew install pyenv
pyenv install 3.11.0
python -m pip install poetry
python -m venv venv
source venv/bin/activate
poetry install
```

NOTE: If you are using VS Code, you will need to set the python interpreter to the "venv" virtual environment that was just created to be able to run the code.

## Getting Started

There are two ways to run the utilities:

1. Programmatically
    - Follow the steps in `demo_GettingStarted.ipynb` in the notebooks folder
2. Web Application
    - Once you have all the dependencies installed, execute the `run.py` function to start the app in development mode. Navigate to the localhost specified in the terminal (e.g., http://127.0.0.1:5000) to run the application locally.
    - I am working on deploying this to production soon...
