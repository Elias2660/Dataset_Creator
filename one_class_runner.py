"""
Module Name: one_class_runner.py

Description:
    Generates partitioned dataset CSVs for a set of videos, treating each video as its own class.
    Reads a counts CSV (with columns "filename" and "framecount"), applies an optional start-frame offset
    and end-frame buffer, then divides each video's usable frames into a specified number of equal splits.
    Outputs:
        - A master "dataset.csv" listing every frame interval with its class label.
        - Individual split files "dataset_0.csv", "dataset_1.csv", ..., each containing one randomly selected
          interval per class, with no duplication across splits.

Usage:
    python one_class_runner.py \
        --in-path <input_directory> \
        --out-path <output_directory> \
        [--counts <counts_csv>] \
        [--start-frame <start_frame>] \
        [--end-frame-buffer <end_frame_buffer>] \
        [--splits <num_splits>]

Arguments:
    --in-path
        Directory containing the counts CSV file. (default: ".")
    --out-path
        Directory where the master and split dataset CSVs will be written. (required)
    --counts
        Filename of the counts CSV, expected to include 'filename' and 'framecount' columns. (default: "counts.csv")
    --start-frame
        Frame index to start from when partitioning. (default: 0)
    --end-frame-buffer
        Number of frames to exclude at the end of each video’s count. (default: 0)
    --splits
        Number of equal segments (splits) into which each video’s usable frames will be divided. (default: 3)

Workflow:
    1. Configure logging and parse command-line arguments.
    2. Load the counts CSV from `<out-path>/<counts>`.
    3. For each video (class), compute the usable frame span:
           usable_frames = framecount - end_frame_buffer - start_frame
           interval_size = usable_frames // splits
    4. Build `dataset.csv` with one row per interval per class (columns: filename, class, beginframe, endframe).
    5. For each split index i in [0, splits):
         a. Randomly select one interval row per class from the master dataset.
         b. Remove those rows from the master set to avoid reuse.
         c. Write the selected intervals to `dataset_i.csv` (columns: file, class, begin frame, end frame).
    6. Log each major step and confirm creation of each output file.

Dependencies:
    - pandas: CSV reading and DataFrame operations.
    - logging: Workflow and error reporting.
    - argparse: Command-line argument parsing.
    - os, random: File path handling and random interval selection.
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

    logging.info(f"Arguments: in_path={args.in_path},"
                 f" out_path={args.out_path}"
                 f" counts={args.counts}, "
                 f" start_frame={args.start_frame}, "
                 f" end_frame_buffer={args.end_frame_buffer}, "
                 f" splits={args.splits}"
                 )

    counts = pd.read_csv(os.path.join(args.out_path, args.counts))

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

    final_dataframe.to_csv(os.path.join(args.out_path, "dataset.csv"), index=False)

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

        dataset_sub.to_csv(os.path.join(args.out_path, f"dataset_{i}.csv"),
                           index=False)
        logging.info(f"dataset_{i}.csv created")
