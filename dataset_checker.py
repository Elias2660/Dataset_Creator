"""
Module Name: dataset_checker.py

Description:
    This module validates and cleans dataset CSV files by performing several checks on each row of the dataset.
    The checks include:
        - Ensuring that the end frame does not exceed the total video frames based on a separate counts CSV.
        - Verifying that there are no missing (null) values in the dataset.
        - Confirming that the begin frame is strictly less than the end frame.
        - Ensuring that both begin and end frames are non-negative.

    If any row fails these validations, detailed error messages are logged, and the problematic rows are removed.
    Prior to updating the dataset, the module creates a backup of the original file with a ".bak" extension.

Usage:
    To run this script from the command line, provide the necessary arguments:
        $ python dataset_checker.py --search-string "dataset_*.csv" --counts "counts.csv"
    - The "--search-string" argument specifies the file pattern to identify the dataset files.
    - The "--counts" argument defines the path to the counts CSV file that contains the reference frame counts.

Functions:
    check_dataset(path: str, counts: pd.DataFrame)
        Reads the dataset from the provided path, performs the necessary validations against the counts DataFrame,
        logs any errors found, and cleans the dataset by removing faulty rows. A backup is created before the cleaned
        dataset overwrites the original.

Logging:
    The module uses Python's logging module to log information about the validation process, including details on errors
    encountered and actions performed (such as creating backups or saving the cleaned dataset).

Notes:
    - The file search is performed using a subprocess call to the "ls" command, which may be platform-dependent (i.e., works
      primarily on Unix-like systems).
    - ANSI escape sequences are stripped from the file search output using regex to ensure clean file names.
"""
import argparse
import logging
import re
import subprocess

import pandas as pd


def check_dataset(path: str, counts: pd.DataFrame):
    """

    :param path: str: 
    :param counts: pd.DataFrame: 

    """
    dataset = pd.read_csv(path)

    faulty_rows = []
    for i in range(len(dataset)):
        if (
            int(dataset.iloc[i, 3])
            > counts[dataset.iloc[i, 0] == counts["filename"]]["framecount"].values[0]
        ):
            # end frame greater than video end frame check
            logging.info(f"-- Error: Found Error Row: {dataset.iloc[i].to_dict()} --")
            logging.info(
                f"Error: Dataset has end frame ({dataset.iloc[i, 3]}) greater than total video frames at row {i}, which is {counts[dataset.iloc[i, 0] == counts['filename']]['framecount'].values[0]}"
            )
            dataset.iloc[i, 3] = counts[dataset.iloc[i, 0] == counts["filename"]][
                "framecount"
            ].values[0]

        if dataset.iloc[i].isnull().values.any():
            # null value check
            logging.info(
                f"\t Error: Found Error Row: {dataset.iloc[i]}".replace("\n", " ")
            )
            logging.info(f"\t Error: Dataset has missing values at row {i}")
            faulty_rows.append(i)
        elif int(dataset.iloc[i, 2]) >= int(dataset.iloc[i, 3]):
            # frame order check
            logging.info(
                f"\t Error: Found Error Row: {dataset.iloc[i]}".replace("\n", " ")
            )
            logging.info(
                f"\t Error: Dataset has begin frame greater than or equal to end frame at row {i}"
            )
            faulty_rows.append(i)
        elif int(dataset.iloc[i, 2]) < 0 or int(dataset.iloc[i, 3]) < 0:
            # negative frame check
            logging.info(
                f"\t Error: Found Error Row: {dataset.iloc[i]}".replace("\n", " ")
            )
            logging.info(
                f"\t Error: Dataset has begin frame or end frame less than or equal to 0 at row {i}"
            )
            faulty_rows.append(i)
    logging.info(f"Found {len(faulty_rows)} faulty rows")

    if len(faulty_rows) == 0:
        logging.info(
            f"Since no faulty rows have been found, dataset is clean and no backups will be made"
        )
        return
    logging.info(f"Cleaning dataset")
    dataset.drop(index=faulty_rows, inplace=True)

    dataset.reset_index(drop=True, inplace=True)
    logging.info(f"Dataset has been cleaned, moving old dataset to backup")
    subprocess.run(f"mv {path} {path}.bak", shell=True)
    logging.info(f"Saving cleaned dataset to {path}")
    dataset.to_csv(path, index=False)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info("Finding Dataset files")

    parser = argparse.ArgumentParser(
        description="Check dataset files for missing values and other errors"
    )
    parser.add_argument(
        "--search-string",
        type=str,
        help="search string to find dataset files",
        default="dataset_*.csv",
    )
    parser.add_argument(
        "--counts", type=str, help="path to counts file", default="counts.csv"
    )

    arguments = parser.parse_args()
    command = f"ls {arguments.search_string}"
    output = subprocess.run(command, shell=True, capture_output=True, text=True)
    # it's weird, but regex is used to find the dataset files
    ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    file_list = sorted(
        [ansi_escape.sub("", line) for line in output.stdout.splitlines()]
    )
    logging.info(f"found dataset files: {file_list}")
    counts = pd.read_csv(arguments.counts)
    for file in file_list:
        # TODO make this check dataset command less verbose
        check_dataset(file, counts)
