"""
Module Name: Make_Dataset.py

Description:
    Combines video frame counts and timestamped log files into a unified dataset CSV.
    Reads a counts CSV (with 'filename' and 'framecount'), processes filenames into timestamps,
    merges with any number of log files assigning class labels, computes begin and end frames
    based on FPS, starting frame offset, class frame intervals, and end-frame buffer.
    Outputs:
        - dataset.csv: aggregated rows with filename, class, beginframe, endframe.
        - RUN_DESCRIPTION.log: records mapping of log files to class numbers.
    Finally, validates the produced dataset using dataset_checker.check_dataset.

Usage:
    python Make_Dataset.py \
        --in-path <input_dir> \
        --out-path <output_dir> \
        [--counts_file <counts_csv>] \
        [--files <log1,log2,...>] \
        [--fps <fps>] \
        [--starting-frame <start_frame>] \
        [--frame-interval <frame_interval>] \
        [--end-frame-buffer <end_frame_buffer>]

Arguments:
    --in-path
        Directory containing counts CSV, log files, and video files. (default: ".")
    --out-path
        Directory for reading inputs and writing outputs. (required)
    --counts_file
        CSV filename with 'filename' and 'framecount' columns. (default: "counts.csv")
    --files
        Comma-separated list of log filenames (e.g., "logNo.txt,logPos.txt,logNeg.txt"). (default: "logNo.txt,logPos.txt,logNeg.txt")
    --fps
        Frames per second for timestamp-to-frame conversion; auto-detected for .mp4, default 25. (default: 25)
    --starting-frame
        Base frame index to start counting from. (default: 1)
    --frame-interval
        Gap in frames to insert between class segments. (default: 0)
    --end-frame-buffer
        Buffer subtracted from the end frame of each video segment. (default: 0)

Workflow:
    1. Configure logging and parse command-line arguments.
    2. Detect or set FPS based on first video file extension.
    3. Load and preprocess counts CSV into timestamped entries (process_frame_count).
    4. Load each log file, convert its frame_name to timestamps, assign incremental class labels (process_log_files).
    5. Merge counts and log entries, sort by time, forward-fill filenames, and compute raw begin/end frames (create_dataset).
    6. Apply starting-frame offset, end-frame buffer, and inter-class frame intervals (add_buffering).
    7. Write out 'dataset.csv' and append class mappings to 'RUN_DESCRIPTION.log'.
    8. Validate and clean the final dataset via dataset_checker.check_dataset.

Dependencies:
    - pandas, numpy: for DataFrame handling and numeric operations.
    - argparse, logging, os: for CLI, logging, and file operations.
    - utils: utility to retrieve video FPS.
    - dataset_checker: to perform final dataset validation and cleanup.
"""

import argparse
import logging
import os

import numpy as np
import pandas as pd

import utils


def process_frame_count(counts: pd.DataFrame, ) -> pd.DataFrame:
    """
    prepare the frame counts to be combined with the other files
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
    processed_counts["beginframe"] = 0
    processed_counts["endframe"] = np.nan
    return processed_counts


def process_log_files(log: pd.DataFrame, classNum: int):
    """

    :param log: pd.DataFrame:
    :param classNum: int:

    prepare a log file to be written to to the dataset.csv file
    :param log: pd.DataFrame:
    :param classNum: int:

    """
    processed_log = pd.DataFrame()
    processed_log["time"] = pd.to_datetime(log["frame_name"],
                                           format="%Y%m%d_%H%M%S")
    processed_log["filename"] = np.nan
    processed_log["class"] = classNum
    processed_log["beginframe"] = np.nan
    processed_log["endframe"] = np.nan
    return processed_log


def create_dataset(
    frame_counts: pd.DataFrame,
    processed_counts: pd.DataFrame,
    FPS: int,
    *args,
) -> pd.DataFrame:
    """

    :param frame_counts: pd.DataFrame:
    :param processed_counts: pd.DataFrame:
    :param FPS: param *args:
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
            row_value = frame_counts.loc[frame_counts["filename"] ==
                                         dataset.loc[i,
                                                     "filename"], "framecount"]
            dataset.loc[i, "endframe"] = row_value.values[0]
            if dataset.loc[i, "beginframe"] != 0:
                dataset.loc[i, "beginframe"] = (
                    # end frame * 2 because the end of the earlier is already subtracted,
                    #  so you have to add it again twice
                    dataset.loc[i - 1, "endframe"])
        elif np.isnan(dataset.loc[i, "beginframe"]) and np.isnan(
                dataset.loc[i, "endframe"]):
            # if it's a switch (e.g. a time object between logNo, logPos, and logNeg)
            # then the end and begin frame are counted on the time difference between the
            # next rows
            dataset.loc[i, "beginframe"] = dataset.loc[i - 1, "endframe"]
            dataset.loc[i, "endframe"] = dataset.loc[i, "beginframe"] + round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds *
                FPS)
        elif dataset.loc[i + 1, "beginframe"] == 0:
            # if it's the video (sourced from the counts.csv), setting the end frame
            # and the row value
            row_value = frame_counts.loc[frame_counts["filename"] ==
                                         dataset.loc[i,
                                                     "filename"], "framecount"]
            dataset.loc[i, "endframe"] = row_value.values[0]
        elif i == 0 and np.isnan(dataset.loc[i, "endframe"]):
            # it's the first frame row and there's no end frame (e.g. it's a video object)
            # the then end frame would be the next row times the fps (this is separate from
            # the next elif because of the issues of being first)
            dataset.loc[i, "endframe"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds *
                FPS)

        elif dataset.loc[i, "beginframe"] == 0 and np.isnan(
                dataset.loc[i, "endframe"]):
            # if it's the starting row, the end frame is the times to the next
            # row times the fps
            dataset.loc[i, "endframe"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds *
                FPS)

        # for classes
        if np.isnan(dataset.loc[i, "class"]) and i == 0:
            # automatically set the first one to class zero if it's first (weird fluke but possible)
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


def add_buffering(dset: pd.DataFrame, starting_frame: int,
                  end_frame_buffer: int, frame_interval: int):
    """
    for each row, update so that the

    Returns:
        buffered dataset (pd.Dataframe)

    Args:
        dset (pd.DataFrame): the dataframe that is outputted by the create_dataset function
        starting_frame (int): the frame that is desired to be started from
        end_frame_buffer (int): the buffer to the end of the video
        frame_interval (int): the buffer between classes
    """
    buffered_dset = dset.copy(deep=True)
    for i in range(len(dset)):
        if i == 0:
            # if i is zero, then just set the initial value to the starting frame
            buffered_dset.loc[i, "beginframe"] = starting_frame

        elif dset.loc[i, "class"] != dset.loc[i - 1, "class"]:
            # add the class frame interval
            # this takes precedent over applying the starting frame and the end frame buffer
            # TODO: might want to make this collaborative, so that this and applying starting_frame and end_frame_buffer
            # TODO: would work in tandem
            buffered_dset.loc[i - 1,
                              "endframe"] = (dset.loc[i - 1, "endframe"] -
                                             frame_interval)
            buffered_dset.loc[i,
                              "beginframe"] = (dset.loc[i - 1, "endframe"] +
                                               frame_interval)

        elif dset.loc[i, "filename"] != dset.loc[i - 1, "filename"]:
            # here, apply the starting_frame and end_frame_buffer
            buffered_dset.loc[i, "beginframe"] = starting_frame
            buffered_dset.loc[i - 1,
                              "endframe"] = (dset.loc[i - 1, "endframe"] -
                                             end_frame_buffer)

    return buffered_dset


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
    1. Parses command-line arguments to get the paths and names of the input files, as well as other parameters like frames per second (FPS), starting frame, frame interval, and end frame buffer.
    2. Reads the counts file (counts.csv) and log files (logNo.txt, logPos.txt, logNeg.txt) from the specified directory.
    3. Processes the counts file to extract and format relevant information such as filenames and timestamps.    for i in len
    4. Processes each log file to extract timestamps and assign class labels (e.g., logNo.txt as class 1, logPos.txt as class 2, logNeg.txt as class 0).
    5. Combines the processed counts and log data into a single dataset, ensuring that the data is sorted by time and that missing values are appropriately handled.
    6. Calculates frame ranges for each entry in the dataset based on the provided FPS, starting frame, frame interval, and end frame buffer.
    7. Outputs the final dataset to a CSV file named dataset.csv in the specified directory.

    This script is useful for preparing data for machine learning models or other analyses that require synchronized and labeled frame data.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--in-path",
        type=str,
        help="path to where directory is located",
        default=".",
        required=False,
    )
    parser.add_argument(
        "--out-path",
        type=str,
        help="path for the output of the workflow"
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
        help=
        "name of the log files that one wants to use, default logNo.txt, logNeg.txt, logPos.txt",
        default="logNo.txt,logPos.txt,logNeg.txt",
        required=False,
    )
    parser.add_argument(
        "--fps",
        type=int,
        help=
        "frames per second, default 25. If mp4, it will be automatically detected",
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
        help="the space between the rows of the dataset of different classes",
        default=0,
        required=False,
    )
    parser.add_argument(
        "--end-frame-buffer",
        type=int,
        help="the buffer given for the end of videos",
        default=0,
        required=False,
    )

    args = parser.parse_args()
    logging.info("Running the Dataset_Creator/Make_Dataset.py script")
    counts_file = args.counts_file
    files = [file.strip() for file in args.files.split(",")]
    dir_files = os.listdir(args.in_path)
    video_files = [
        file for file in dir_files
        if file.endswith(".mp4") or file.endswith(".h264")
    ]
    
    if (len(video_files) == 0):
        raise Exception("This process found no video files. "
                        "You might have specified the wrong path. "
                        "Otherwise, there might be an issue in the Unified-bee-Runner pipeline")

    if video_files[0].endswith(".mp4"):
        fps = utils.get_video_info(video_files, args.in_path)
        logging.info(f"Found fps {fps}")
    elif video_files[0].endswith(".h264"):
        # this is because finding the frames per second of a .h264 file is a pain in the ass
        fps = args.fps
        logging.info(f"Using fps {fps}")

    counts = pd.read_csv(os.path.join(args.out_path, counts_file))
    processed_counts = process_frame_count(counts)
    list_of_logs = []  # allow for any number of log files
    
    if len(list_of_logs == 0):
        raise Exception("There are no log files specified. "
                        "You might have forgotten to add them or may have accidentally deleted them. " 
                        "If this was intentional (e.g. every video is one class) you might need to specify another option")
        
    if len(list_of_logs == 1):
        raise Warning("You have only one specified log file open. This might cause issues because most of the videos might be one class"
                      "If this was intentional (e.g. every video is one class) you might need to specify another option. "
                      "Otherwise, Ignore this warning. "
                      )
    
    class_idx = 0

    # add class-dataset class name relations to RUN_DESCRIPTION.log for clarity
    with open(os.path.join(args.out_path, "RUN_DESCRIPTION.log"), "a+") as rd:
        rd.write(f"\n-- Class Relations --\n")

    for file in files:
        logging.info(
            f"Assigning class number {class_idx} to class {(file.split('.')[0][3:]).upper()}"
        )

        with open(os.path.join(args.out_path, "RUN_DESCRIPTION.log"), "a+") as rd:
            rd.write(
                f"Assigning class number {class_idx} to class {(file.split('.')[0][3:]).upper()} \n"
            )

        logFile = pd.read_csv(os.path.join(args.in_path, file), names=["frame_name"])
        processed_logfile = process_log_files(logFile, class_idx)
        list_of_logs.append(processed_logfile)

        class_idx += 1

    dset = create_dataset(
        counts,
        processed_counts,
        fps,
        *list_of_logs,
    )

    # add the frame interval, begin frame, and end frame aspects to the dataset
    dset = add_buffering(
        dset,
        args.starting_frame,
        args.end_frame_buffer,
        args.frame_interval,
    )

    dset.to_csv(os.path.join(args.out_path, "dataset.csv"), index=False)
    # check using dataset_checker.py
    from dataset_checker import check_dataset

    # the dataset algo automatically can create inconsistencies
    check_dataset(os.path.join(args.out_path, "dataset.csv"), counts)
