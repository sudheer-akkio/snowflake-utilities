import os

import pandas as pd
from sklearn.model_selection import TimeSeriesSplit, train_test_split


def partition_data(
    filename, out_location=os.getcwd(), test_partition=0.1, shuffle=True
):
    """Partition data to generate a train / test split"""

    fname = os.path.splitext(filename)[0]
    ext = os.path.splitext(filename)[1]

    df = pd.read_csv(filename)

    training_data, testing_data = train_test_split(
        df, test_size=test_partition, shuffle=shuffle, random_state=25
    )

    print(f"No. of training examples: {training_data.shape[0]}")
    print(f"No. of testing examples: {testing_data.shape[0]}")

    train_filename = os.path.join(out_location, fname + "-train" + ext)
    test_filename = os.path.join(out_location, fname + "-test" + ext)

    training_data.to_csv(train_filename, index=False)
    testing_data.to_csv(test_filename, index=False)


def partition_time_series(
    filename, time_varname, response, out_location=os.getcwd(), n_splits=2, test_size=20
):
    """Partition time series data into training / test splits"""

    fname = os.path.splitext(filename)[0]
    ext = os.path.splitext(filename)[1]

    df = pd.read_csv(filename)

    df[time_varname] = pd.to_datetime(df[time_varname])
    df.set_index(time_varname, inplace=True)
    df.sort_index(inplace=True)

    tss = TimeSeriesSplit(n_splits=n_splits, test_size=test_size)

    X = df.drop(labels=[response], axis=1)
    y = df[response]

    for train_index, test_index in tss.split(X):
        X_train, X_test = X.iloc[train_index, :], X.iloc[test_index, :]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]

    training_data = pd.concat([X_train, y_train], axis=1)
    testing_data = pd.concat([X_test, y_test], axis=1)

    train_filename = os.path.join(out_location, fname + "-train" + ext)
    test_filename = os.path.join(out_location, fname + "-test" + ext)

    training_data.to_csv(train_filename)
    testing_data.to_csv(test_filename)


filename = "/Users/snuggeha/Documents/Internal-Demos/snowflake-utilities/data/DR_Demo_Sales_Multiseries.csv"

partition_time_series(filename, "Date", "Sales")
