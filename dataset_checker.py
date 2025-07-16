"""
Module Name: dataset_checker.py

Description:
    Scans a directory for dataset CSV files matching a given glob pattern and validates each file
    against reference frame counts from a separate counts CSV. For each row in each dataset file, the following checks are performed:
        - End frame does not exceed the total frame count for the corresponding video.
        - No missing (null) values.
        - Begin frame is strictly less than the end frame.
        - Begin and end frames are non-negative.
    Detailed error messages are logged at INFO level. Any rows that fail validation are dropped,
    and the cleaned dataset overwrites the original file only after creating a backup with a “.bak” extension.

Usage:
    $ python dataset_checker.py \
        --in-path <input_directory> \
        --search-string "<pattern>" \
        --counts <counts_csv> \
        [--out-path <output_directory>]

    Arguments:
        --in-path        Directory to search for dataset files and the counts CSV (default: current directory)
        --search-string  Glob pattern to match dataset CSV files (default: "dataset_*.csv")
        --counts         Filename of the counts CSV within the input directory (default: "counts.csv")
        --out-path       Directory to save cleaned datasets and backups (currently the same as --in-path)

Functions:
    check_dataset(path: str, counts: pd.DataFrame)
        Reads the dataset at `path`, validates rows against the `counts` DataFrame, logs any errors,
        drops faulty rows, creates a backup (`path + ".bak"`), and writes the cleaned dataset back to `path`.

Logging:
    Uses Python’s built-in `logging` module configured at INFO level with timestamped messages.
"""

import argparse
import logging
import re
import subprocess
import os

import pandas as pd


def check_dataset(path: str, counts: pd.DataFrame):
    """

    :param path: str:
    :param counts: pd.DataFrame:

    """
    dataset = pd.read_csv(path)

    faulty_rows = []
    for i in range(len(dataset)):
        if (int(dataset.iloc[i, 3]) > counts[dataset.iloc[
                i, 0] == counts["filename"]]["framecount"].values[0]):
            # end frame greater than video end frame check
            logging.info(
                f"Error in row {dataset.iloc[i].to_dict()}: Dataset has end frame ({dataset.iloc[i, 3]}) greater than total video frames at row {i}, which is {counts[dataset.iloc[i, 0] == counts['filename']]['framecount'].values[0]}"
            )
            dataset.iloc[i, 3] = counts[dataset.iloc[
                i, 0] == counts["filename"]]["framecount"].values[0]

        if dataset.iloc[i].isnull().values.any():
            # null value check
            logging.info(
                f"\t Error: Found Error at Row {i}: {dataset.iloc[i]}: missing values at row"
                .replace("\n", " ").replace("  ", " "))
            faulty_rows.append(i)
        elif int(dataset.iloc[i, 2]) >= int(dataset.iloc[i, 3]):
            # frame order check
            logging.info(
                f"\t Error: Found Error at row {i}: {dataset.iloc[i]}: begin frame greater than or equal to end frame"
                .replace("\n", " "))
            faulty_rows.append(i)
        elif int(dataset.iloc[i, 2]) < 0 or int(dataset.iloc[i, 3]) < 0:
            # negative frame check
            logging.info(
                f"\t Error: Found Error at row {i}: {dataset.iloc[i]}: begin frame or end frame less than or equal to 0"
                .replace("\n", " "))
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
        description="Check dataset files for missing values and other errors")
    parser.add_argument(
        "--in-path",
        type=str,
        help="the path to find the input files for the directory",
        default="."
    )
    parser.add_argument(
        "--out-path",
        type=str,
        help="the path to find the output files for the directory",
        default="."
    )
    parser.add_argument(
        "--search-string",
        type=str,
        help="search string to find dataset files",
        default="dataset_*.csv",
    )
    parser.add_argument("--counts",
                        type=str,
                        help="name of the file with frame count per video",
                        default="counts.csv")

    arguments = parser.parse_args()
    command = f"ls {os.path.join(arguments.in_path, arguments.search_string)}"
    output = subprocess.run(command,
                            shell=True,
                            capture_output=True,
                            text=True)
    # it's weird, but regex is used to find the dataset files
    ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    file_list = sorted(
        [ansi_escape.sub("", line).split("/")[-1] for line in output.stdout.splitlines()])
    logging.info(f"found dataset files: {file_list}")
    counts = pd.read_csv(os.path.join(arguments.in_path, arguments.counts))
    for file in file_list:
        check_dataset(os.path.join(arguments.in_path, file), counts)
