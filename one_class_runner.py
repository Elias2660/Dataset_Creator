"""
one_class_runner.py

This script processes a counts csv file containing video information and generates dataset CSV files by partitioning
each video's frame count into several splits. Each video is treated as a distinct class, and for each class,
the script creates frame intervals and produces multiple dataset files with one frame interval per class per file.

This is meant to run with testing scripts that create one video for each class.

Usage:
    Run the script from the command line in the following format:
        python one_class_runner.py [--path PATH] [--counts COUNTS_CSV]
                                    [--start-frame START_FRAME] [--end-frame-buffer END_FRAME_BUFFER]
                                    [--splits SPLITS]

Arguments:
    --path:
        Path to the directory containing the counts CSV file.
        (Default: ".")

    --counts:
        Filename for the counts CSV file, expected to have columns including at least 'filename' and 'framecount'.
        (Default: "counts.csv")

    --start-frame:
        The starting frame number to consider when splitting frames.
        (Default: 0)

    --end-frame-buffer:
        Number of frames to reserve at the end, effectively reducing the total frames considered.
        (Default: 0)

    --splits:
        Number of splits (or segments) per video. The script divides each video's usable frames into this many segments.
        (Default: 3)

Workflow:
    1. Initialize logging and parse command line arguments.
    2. Read the counts CSV file located in the specified directory.
    3. For each video file (treated as a unique class), compute frame intervals by subtracting buffers and dividing
       by the number of splits.
    4. Create a master dataset CSV file ("dataset.csv") that records all frame ranges, filenames, and associated class labels.
    5. For each split:
         - Randomly select one interval per class.
         - Remove the chosen interval from the master dataset to avoid duplication.
         - Write the selected intervals to a new CSV file (e.g., "dataset_0.csv", "dataset_1.csv", etc.).
    6. Log key processing steps and output file creation progress.

Dependencies:
    - pandas: For reading and manipulating CSV data.
    - logging: For logging information throughout the processing.
    - argparse: For handling command line argument parsing.
    - os: For file path operations.
    - random: For random selection of intervals during dataset file creation.
"""
import argparse
import logging
import os
import random

import pandas as pd

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description=
        "Special case for dataprep videos where each video is one different class"
    )
    parser.add_argument(
        "--path",
        type=str,
        help="path to where directory is located",
        default=".",
        required=False,
    )
    parser.add_argument(
        "--counts",
        type=str,
        help="name of counts file, default is counts.csv",
        default="counts.csv",
        required=False,
    )
    parser.add_argument(
        "--start-frame",
        type=int,
        help="start frame, default zero",
        default=0,
        required=False,
    )
    parser.add_argument(
        "--end-frame-buffer",
        type=int,
        help="the end frame that one would use, default 0",
        default=0,
        required=False,
    )
    parser.add_argument(
        "--splits",
        type=int,
        help="number of splits per video, default 3",
        default=3,
        required=False,
    )
    args = parser.parse_args()

    logging.info("Running the Dataset_Creator/one_class_runner.py script")
    logging.info("Finding Dataset files")

    logging.info(f"Arguments: path={args.path},"
                 f" counts={args.counts}, "
                 f" start_frame={args.start_frame}, "
                 f"end_frame_buffer={args.end_frame_buffer}, "
                 f" splits={args.splits}")

    counts = pd.read_csv(os.path.join(args.path, args.counts))

    final_dataframe = pd.DataFrame(
        columns=["filename", "class", "beginframe", "endframe"])
    class_count = 0

    for row in counts.iterrows():
        frame_interval = (row[1]["framecount"] - args.end_frame_buffer -
                          args.start_frame) // args.splits
        begin_frame = args.start_frame
        end_frame = frame_interval
        for split in range(args.splits):
            final_dataframe = pd.concat(
                [
                    final_dataframe,
                    pd.DataFrame([{
                        "filename": row[1]["filename"],
                        "class": class_count,
                        "beginframe": begin_frame,
                        "endframe": end_frame,
                    }]),
                ],
                ignore_index=True,
            )
            begin_frame += frame_interval
            end_frame += frame_interval

        class_count += 1

    final_dataframe.to_csv(os.path.join(args.path, "dataset.csv"), index=False)

    for i in range(args.splits):
        logging.info(f"Creating dataset_{i}.csv")
        # for each class, create a dataset file
        dataset_sub = pd.DataFrame(
            columns=["file", "class", "begin frame", "end frame"])
        for class_num in range(0, class_count):
            # find all rows with class equal to class count
            class_rows = final_dataframe[final_dataframe["class"] == class_num]
            if not class_rows.empty:
                row_num = class_rows.index[0]
                if len(class_rows) > 1:
                    row_num = random.choice(class_rows.index.tolist())

                row = class_rows.loc[[row_num]]

                # remove the rows from the final_dataframe
                final_dataframe = final_dataframe.drop(row_num)
                final_dataframe = final_dataframe.reset_index(drop=True)
                # add the rows to the new dataframe

                dataset_sub = pd.concat(
                    [
                        dataset_sub,
                        row.rename(
                            columns={
                                "filename": "file",
                                "beginframe": "begin frame",
                                "endframe": "end frame",
                            }),
                    ],
                    ignore_index=True,
                )

        dataset_sub.to_csv(os.path.join(args.path, f"dataset_{i}.csv"),
                           index=False)
        logging.info(f"dataset_{i}.csv created")
