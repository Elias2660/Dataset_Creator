"""
Make_Dataset.py

Description:
    This script creates a comprehensive dataset CSV file ("dataset.csv") by combining frame count data from "counts.csv"
    with log file data (logNo.txt, logPos.txt, logNeg.txt). It processes the frame counts by converting video filenames into
    timestamps and then merges these with the log files that assign class labels. The final CSV aggregates each video's filename,
    class label, beginning frame, and ending frame based on calculated frame intervals determined by the provided FPS,
    starting frame, and frame interval parameters.

Usage:
    Run the script from the command line as follows:
        python Make_Dataset.py [--path PATH] [--counts_file COUNTS_CSV] [--files LOG_FILES]
                               [--fps FPS] [--starting-frame START_FRAME] [--frame-interval FRAME_INTERVAL]

Arguments:
    --path:
        Directory path where the input files are located.
        (Default: ".")

    --counts_file:
        Filename of the counts CSV file containing 'filename' and 'framecount' columns.
        (Default: "counts.csv")

    --files:
        Comma-separated list of log file names to process (e.g., "logNo.txt,logPos.txt,logNeg.txt").
        (Default: "logNo.txt,logPos.txt,logNeg.txt")

    --fps:
        Frames per second used in the video. Defaults to 25 or is auto-detected based on the video file type.
        (Default: 25)

    --starting-frame:
        Starting frame number used as the basis for computing frame intervals.
        (Default: 1)

    --frame-interval:
        Number of frames to add as an interval between segments.
        (Default: 0)

Workflow:
    1. Sets up logging and parses command-line arguments.
    2. Reads the "counts.csv" file and the provided log files from the specified directory.
    3. Processes the counts file by converting video filenames to timestamps and initializing frame parameters.
    4. Processes each log file to extract timestamps and assign corresponding class labels.
    5. Merges the processed counts and log data, filling missing filenames and sorting entries by time.
    6. Calculates the beginning and ending frames for each dataset entry based on the FPS, starting frame, and frame interval.
    7. Outputs the final dataset to "dataset.csv" and verifies its integrity using an external checker.

Dependencies:
    - pandas: For CSV manipulation and data processing.
    - numpy: For numerical operations and handling missing values.
    - argparse: For parsing command-line arguments.
    - logging: For logging key steps during script execution.
    - os: For handling file system operations.
    - utils: For additional utility functions such as fetching video metadata.

Returns:
    A CSV file ("dataset.csv") that aggregates each video's filename, class label, beginning frame, and ending frame.
"""

import argparse
import os
import numpy as np
import pandas as pd
import utils
import logging


def process_frame_count(counts: pd.DataFrame, starting_frame: int) -> pd.DataFrame:
    """

    :param counts: pd.DataFrame
    :param starting_frame: int

    prepare the frame counts to be combined with the other files
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

    prepare a log file to be written to to the dataset.csv file
    """
    processed_log = pd.DataFrame()
    processed_log["time"] = pd.to_datetime(log["frame_name"], format="%Y%m%d_%H%M%S")
    processed_log["filename"] = np.nan
    processed_log["class"] = classNum
    processed_log["beginframe"] = np.nan
    processed_log["endframe"] = np.nan
    return processed_log


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

    # let's separate into video rows and change rows.
    # IMPORTANT this method, especially if you have a begin-frame greater than zero
    # can cause the begin frame to be higher than the end frame. This will get rooted
    # out in the dataset checker
    for i in range(len(dataset)):
        if i == len(dataset) - 1:
            # if it's the last row, then do something special
            row_value = frame_counts.loc[
                frame_counts["filename"] == dataset.loc[i, "filename"], "framecount"
            ]
            dataset.loc[i, "endframe"] = row_value.values[0] - frame_interval
            if dataset.loc[i, "beginframe"] != starting_frame:
                dataset.loc[i, "beginframe"] = (
                    # end frame * 2 because the end of the earlier is already subtracted,
                    #  so you have to add it again twice
                    dataset.loc[i - 1, "endframe"]
                    + frame_interval * 2
                )
        elif np.isnan(dataset.loc[i, "beginframe"]) and np.isnan(
            dataset.loc[i, "endframe"]
        ):
            # if it's a switch (e.g. a time object between logNo, logPos, and logNeg)
            # then the end and begin frame are counted on the time difference between the
            # next rows
            dataset.loc[i, "beginframe"] = (
                dataset.loc[i - 1, "endframe"] + frame_interval * 2
            )
            dataset.loc[i, "endframe"] = (
                dataset.loc[i, "beginframe"]
                + round(
                    (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
                )
                - frame_interval
            )
        elif dataset.loc[i + 1, "beginframe"] == starting_frame:
            # if it's the video (sourced from the counts.csv), setting the end frame
            # and the row value
            row_value = frame_counts.loc[
                frame_counts["filename"] == dataset.loc[i, "filename"], "framecount"
            ]
            dataset.loc[i, "endframe"] = row_value.values[0] - frame_interval
        elif i == 0 and np.isnan(dataset.loc[i, "endframe"]):
            # it's the first frame row and there's no end frame (e.g. it's a video object)
            # the then end frame would be the next row times the fps (this is separate from
            # the next elif because of the issues of being first)
            dataset.loc[i, "endframe"] = (
                round(
                    (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
                )
                - frame_interval
            )

        elif dataset.loc[i, "beginframe"] == starting_frame and np.isnan(
            dataset.loc[i, "endframe"]
        ):
            # if it's the starting row, the end frame is the times to the next
            # row times the fps
            dataset.loc[i, "endframe"] = (
                round(
                    (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
                )
                - frame_interval
            )

        # for classes
        if np.isnan(dataset.loc[i, "class"]) and i == 0:
            # automatically set the first one to class zero if it's first (wired fluke but possible)
            dataset.loc[i, "class"] = 0
        elif np.isnan(dataset.loc[i, "class"]):
            # else just set it to the class above it
            dataset.loc[i, "class"] = dataset.loc[i - 1, "class"]

    # for endframes
    dataset["class"] = dataset["class"].astype(int)
    dataset["beginframe"] = dataset["beginframe"].astype(int)
    dataset["endframe"] = dataset["endframe"].astype(int)
    dataset = dataset.drop(columns=["time"])
    return dataset


if __name__ == "__main__":

    logging.basicConfig(
        format="%(asctime)s: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
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
    parser.add_argument(
        "--end-frame-buffer",
        type=int,
        help="the buffer for each dataset.csv end frame",
        default=0,
        required=False,
    )

    args = parser.parse_args()
    logging.info("Running the Dataset_Creator/Make_Dataset.py script")
    path = args.path
    counts_file = args.counts_file
    files = [file.strip() for file in args.files.split(",")]
    dir_files = os.listdir(path)
    video_files = [
        file for file in dir_files if file.endswith(".mp4") or file.endswith(".h264")
    ]

    if video_files[0].endswith(".mp4"):
        fps = utils.get_video_info(files, path)
    elif video_files[0].endswith(".h264"):
        # this is because finding the frames per second of a .h264 file is a pain in the ass
        fps = 25

    counts = pd.read_csv(os.path.join(path, counts_file))
    processed_counts = process_frame_count(counts, args.starting_frame)
    list_of_logs = []  # allow for any number of log files

    class_idx = 0

    # add class-dataset class name relations to RUN_DESCRIPTION.log for clarity
    with open(os.path.join(args.path, "RUN_DESCRIPTION.log"), "a+") as rd:
        rd.write(f"\n-- Class Relations --\n")

    for file in files:
        logging.info(
            f"Assigning class number {class_idx} to class {(file.split('.')[0][3:]).upper()}"
        )

        with open(os.path.join(args.path, "RUN_DESCRIPTION.log"), "a+") as rd:
            rd.write(
                f"Assigning class number {class_idx} to class {(file.split('.')[0][3:]).upper()} \n"
            )

        logFile = pd.read_csv(os.path.join(path, file), names=["frame_name"])
        processed_logfile = process_log_files(logFile, class_idx)
        list_of_logs.append(processed_logfile)

        class_idx += 1

    dset = create_dataset(
        counts,
        processed_counts,
        fps,
        args.starting_frame,
        args.frame_interval,
        *list_of_logs,
    )

    for dset_idx in range(len(dset.index)):
        dset.loc[dset_idx].endframe -= args.end_frame_buffer

    dset.to_csv(os.path.join(path, "dataset.csv"), index=False)
    # check using dataset_checker.py
    from dataset_checker import check_dataset

    # the dataset algo automatically can create inconsistencies
    check_dataset(os.path.join(path, "dataset.csv"), counts)
