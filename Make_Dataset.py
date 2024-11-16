import argparse
import os
import numpy as np
import pandas as pd
import utils


def process_frame_count(counts: pd.DataFrame, starting_frame: int) -> pd.DataFrame:
    """

    :param counts: pd.DataFrame:

    """
    processed_counts = pd.DataFrame()
    nc = counts.copy()
    nc["filename"] = nc["filename"].str.replace(".h264", "", regex=False)
    nc["filename"] = nc["filename"].str.replace(".mp4", "", regex=False)
    processed_counts["time"] = pd.to_datetime(
        nc["filename"],
        format="%Y-%m-%d %H:%M:%S.%f",
    )
    processed_counts["filename"] = counts["filename"]
    processed_counts["class"] = np.nan
    processed_counts["beginframe"] = starting_frame
    processed_counts["endframe"] = np.nan
    return processed_counts


def process_log_files(log: pd.DataFrame, classNum: int):
    """

    :param log: pd.DataFrame:
    :param classNum: int:

    """
    processed_log = pd.DataFrame()
    processed_log["time"] = pd.to_datetime(log["frame_name"], format="%Y%m%d_%H%M%S")
    processed_log["filename"] = np.nan
    processed_log["class"] = classNum
    processed_log["beginframe"] = np.nan
    processed_log["endframe"] = np.nan
    return processed_log


LOG_NEG_CLASS_VALUE = 0
LOG_NO_CLASS_VALUE = 1
LOG_POS_CLASS_VALUE = 2


def create_dataset(
    frame_counts: pd.DataFrame,
    processed_counts: pd.DataFrame,
    FPS: int,
    starting_frame: int,
    frame_interval: int,
    *args,
) -> pd.DataFrame:
    """
    :param frame_counts: pd.DataFrame:
    :param processed_counts: pd.DataFrame:
    :param FPS: param *args:
    :param frame_counts: pd.DataFrame:
    :param processed_counts: pd.DataFrame:
    :param *args:

    """
    dataset = pd.concat([processed_counts, *args], ignore_index=True)
    dataset = dataset.sort_values(by="time").reset_index(drop=True)
    # for filenames
    dataset["filename"] = dataset["filename"].ffill()
    dataset = dataset.dropna(subset=["filename"]).reset_index(drop=True)
    # for frames
    for i in range(len(dataset)):
        if i == len(dataset) - 1:
            row_value = frame_counts.loc[
                frame_counts["filename"] == dataset.loc[i, "filename"], "framecount"
            ]
            dataset.loc[i, "endframe"] = row_value.values[0]
            if dataset.loc[i, "beginframe"] != starting_frame:
                dataset.loc[i, "beginframe"] = (
                    dataset.loc[i - 1, "endframe"] + 1 + frame_interval
                )
        elif np.isnan(dataset.loc[i, "beginframe"]) and np.isnan(
            dataset.loc[i, "endframe"]
        ):
            dataset.loc[i, "beginframe"] = (
                dataset.loc[i - 1, "endframe"] + 1 + frame_interval
            )
            dataset.loc[i, "endframe"] = dataset.loc[i, "beginframe"] + round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
            )
        elif dataset.loc[i + 1, "beginframe"] == starting_frame:
            row_value = frame_counts.loc[
                frame_counts["filename"] == dataset.loc[i, "filename"], "framecount"
            ]
            dataset.loc[i, "endframe"] = row_value.values[0]
        elif i == 0 and np.isnan(dataset.loc[i, "endframe"]):
            dataset.loc[i, "endframe"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
            )

        elif dataset.loc[i, "beginframe"] == starting_frame and np.isnan(
            dataset.loc[i, "endframe"]
        ):
            dataset.loc[i, "endframe"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
            )

        # for classes
        if np.isnan(dataset.loc[i, "class"]) and i == 0:
            dataset.loc[i, "class"] = LOG_NO_CLASS_VALUE
        elif np.isnan(dataset.loc[i, "class"]):
            dataset.loc[i, "class"] = dataset.loc[i - 1, "class"]

    # for endframes
    dataset["class"] = dataset["class"].astype(int)
    dataset["beginframe"] = dataset["beginframe"].astype(int)
    dataset["endframe"] = dataset["endframe"].astype(int)
    dataset = dataset.drop(columns=["time"])
    return dataset


if __name__ == "__main__":
    description = """
Create Dataset File

This script is used to create a comprehensive dataset file from multiple input files including counts.csv, logNo.txt, logPos.txt, and logNeg.txt. 

The script performs the following steps:
1. Parses command-line arguments to get the paths and names of the input files, as well as other parameters like frames per second (FPS), starting frame, and frame interval.
2. Reads the counts file (counts.csv) and log files (logNo.txt, logPos.txt, logNeg.txt) from the specified directory.
3. Processes the counts file to extract and format relevant information such as filenames and timestamps.
4. Processes each log file to extract timestamps and assign class labels (e.g., logNo.txt as class 1, logPos.txt as class 2, logNeg.txt as class 0).
5. Combines the processed counts and log data into a single dataset, ensuring that the data is sorted by time and that missing values are appropriately handled.
6. Calculates frame ranges for each entry in the dataset based on the provided FPS, starting frame, and frame interval.
7. Outputs the final dataset to a CSV file named dataset.csv in the specified directory.

This script is useful for preparing data for machine learning models or other analyses that require synchronized and labeled frame data.
"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--path",
        type=str,
        help="path to the directory where the files are located, default .",
        default=".",
        required=False,
    )
    parser.add_argument(
        "--counts_file",
        type=str,
        help="name of the counts file, default counts.csv",
        default="counts.csv",
        required=False,
    )
    parser.add_argument(
        "--files",
        type=str,
        help="name of the log files that one wants to use, default logNo.txt, logNeg.txt, logPos.txt",
        default="logNo.txt,logPos.txt,logNeg.txt",
        required=False,
    )
    parser.add_argument(
        "--fps",
        type=int,
        help="frames per second, default 25. If mp4, it will be automatically detected",
        default=25,
        required=False,
    )
    parser.add_argument(
        "--starting-frame",
        type=int,
        help="starting frame, default 1",
        default=1,
        required=False,
    )
    parser.add_argument(
        "--frame-interval",
        type=int,
        help="space between frames, default 0",
        default=0,
        required=False,
    )

    args = parser.parse_args()

    path = args.path
    counts_file = args.counts_file
    files = [file.strip() for file in args.files.split(",")]
    video_files = [
        file for file in files if file.endswith(".mp4") or file.endswith(".h264")
    ]
    if video_files[0].endswith(".mp4"):
        fps = utils.get_video_info(files, path)
    elif video_files[0].endswith(".h264"):
        fps = 25

    counts = pd.read_csv(os.path.join(path, counts_file))
    if "logNo.txt" in files:
        logNo = pd.read_csv(os.path.join(path, "logNo.txt"), names=["frame_name"])
    if "logPos.txt" in files:
        logPos = pd.read_csv(os.path.join(path, "logPos.txt"), names=["frame_name"])
    if "logNeg.txt" in files:
        logNeg = pd.read_csv(os.path.join(path, "logNeg.txt"), names=["frame_name"])

    processed_counts = process_frame_count(counts, args.starting_frame)

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

    dset = create_dataset(
        counts,
        processed_counts,
        fps,
        args.starting_frame,
        args.frame_interval,
        *list_of_logs,
    )
    dset.to_csv(os.path.join(path, "dataset.csv"), index=False)
    # check using dataset_checker.py
    from dataset_checker import check_dataset

    check_dataset(os.path.join(path, "dataset.csv"), counts)
