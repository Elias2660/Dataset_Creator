import argparse
import os

import numpy as np
import pandas as pd
"""
the format of the new processed dataframe would be
columns =  time, name, class, begin frame, end frame,
"""


def process_frame_count(counts: pd.DataFrame):
    """

    :param counts: pd.DataFrame:

    """
    processed_counts = pd.DataFrame()
    nc = counts.copy()
    nc["filename"] = nc["filename"].str.replace(".h264", "", regex=False)
    nc["filename"] = nc["filename"].str.replace(".mp4", "", regex=False)
    print(nc)
    processed_counts["time"] = pd.to_datetime(
        nc["filename"],
        format="%Y-%m-%d %H:%M:%S.%f",
    )
    processed_counts["filename"] = counts["filename"]
    processed_counts["class"] = np.nan
    processed_counts["begin frame"] = 0
    processed_counts["end frame"] = np.nan
    return processed_counts


def process_log_files(log: pd.DataFrame, classNum: int):
    """

    :param log: pd.DataFrame:
    :param classNum: int:

    """
    processed_log = pd.DataFrame()
    processed_log["time"] = pd.to_datetime(log["frame_name"],
                                           format="%Y%m%d_%H%M%S")
    processed_log["filename"] = np.nan
    processed_log["class"] = classNum
    processed_log["begin frame"] = np.nan
    processed_log["end frame"] = np.nan
    return processed_log


LOG_NEG_CLASS_VALUE = 0
LOG_NO_CLASS_VALUE = 1
LOG_POS_CLASS_VALUE = 2


def create_dataset(frame_counts: pd.DataFrame, processed_counts: pd.DataFrame,
                   FPS, *args):
    """

    :param frame_counts: pd.DataFrame:
    :param processed_counts: pd.DataFrame:
    :param FPS: param *args:
    :param frame_counts: pd.DataFrame:
    :param processed_counts: pd.DataFrame:
    :param *args:

    """
    print(len([*args]))
    dataset = pd.concat([processed_counts, *args], ignore_index=True)
    dataset = dataset.sort_values(by="time").reset_index(drop=True)
    # for filenames
    dataset["filename"] = dataset["filename"].ffill()
    dataset = dataset.dropna(subset=["filename"]).reset_index(drop=True)
    # for frames
    for i in range(len(dataset)):
        if i == len(dataset) - 1:
            row_value = frame_counts.loc[frame_counts["filename"] ==
                                         dataset.loc[i, "filename"],
                                         "framecount"]
            dataset.loc[i, "end frame"] = row_value.values[0]
            if dataset.loc[i, "begin frame"] != 0:
                dataset.loc[i, "begin frame"] = dataset.loc[i - 1,
                                                            "end frame"] + 1
        elif np.isnan(dataset.loc[i, "begin frame"]) and np.isnan(
                dataset.loc[i, "end frame"]):
            dataset.loc[i, "begin frame"] = dataset.loc[i - 1, "end frame"] + 1
            dataset.loc[i,
                        "end frame"] = dataset.loc[i, "begin frame"] + round(
                            (dataset.loc[i + 1, "time"] -
                             dataset.loc[i, "time"]).seconds * FPS)
        elif dataset.loc[i + 1, "begin frame"] == 0:
            row_value = frame_counts.loc[frame_counts["filename"] ==
                                         dataset.loc[i, "filename"],
                                         "framecount"]
            dataset.loc[i, "end frame"] = row_value.values[0]
        elif i == 0 and np.isnan(dataset.loc[i, "end frame"]):
            dataset.loc[i, "end frame"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds *
                FPS)

        elif dataset.loc[i, "begin frame"] == 0 and np.isnan(
                dataset.loc[i, "end frame"]):
            dataset.loc[i, "end frame"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds *
                FPS)

        # for classes
        if np.isnan(dataset.loc[i, "class"]) and i == 0:
            dataset.loc[i, "class"] = LOG_NO_CLASS_VALUE
        elif np.isnan(dataset.loc[i, "class"]):
            dataset.loc[i, "class"] = dataset.loc[i - 1, "class"]

    # for end frames
    print(dataset.tail())
    dataset["class"] = dataset["class"].astype(int)
    dataset["begin frame"] = dataset["begin frame"].astype(int)
    dataset["end frame"] = dataset["end frame"].astype(int)
    dataset = dataset.drop(columns=["time"])
    return dataset


description = """
Create Dataset File

This script is used to create a dataset file from the counts.csv, logNo.txt, logPos.txt, and logNeg.txt files.
"""
# Load the data

parser = argparse.ArgumentParser(description=description)
parser.add_argument(
    "--path",
    type=str,
    help="path to the directory where the files are located, default .",
    default=".",
)
parser.add_argument(
    "--counts_file",
    type=str,
    help="name of the counts file, default counts.csv",
    default="counts.csv",
)
parser.add_argument(
    "--files",
    type=str,
    help=
    "name of the log files that one wants to use, default logNo.txt, logNeg.txt, logPos.txt",
    default="logNo.txt, logPos.txt, logNeg.txt",
)
parser.add_argument("--fps",
                    type=int,
                    help="frames per second, default 25",
                    default=25)

args = parser.parse_args()
path = args.path
counts_file = args.counts_file
files = [file.strip() for file in args.files.split(", ")]
fps = args.fps

counts = pd.read_csv(os.path.join(path, counts_file))
if "logNo.txt" in files:
    logNo = pd.read_csv(os.path.join(path, "logNo.txt"), names=["frame_name"])
if "logPos.txt" in files:
    logPos = pd.read_csv(os.path.join(path, "logPos.txt"),
                         names=["frame_name"])
if "logNeg.txt" in files:
    logNeg = pd.read_csv(os.path.join(path, "logNeg.txt"),
                         names=["frame_name"])

processed_counts = process_frame_count(counts)

list_of_logs = []

if "logNo.txt" in files:
    processed_logNo = process_log_files(logNo, 1)
    list_of_logs.append(processed_logNo)

if "logPos.txt" in files:
    processed_logPos = process_log_files(logPos, 2)
    list_of_logs.append(processed_logPos)

if "logNeg.txt" in files:
    processed_logNeg = process_log_files(logNeg, 0)
    list_of_logs.append(processed_logNeg)

dset = create_dataset(counts, processed_counts, fps, *list_of_logs)
print(dset)
dset.to_csv(os.path.join(path, "dataset.csv"), index=False)