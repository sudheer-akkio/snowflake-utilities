import os

import pandas as pd
from sklearn.model_selection import train_test_split


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
