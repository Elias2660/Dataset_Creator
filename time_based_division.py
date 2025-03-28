"""
time_based_division.py

Description:
    This script reads a CSV file ("counts.csv") containing video information and divides the videos into a specified 
    number of classes based on the provided splits. For each video, it computes a frame interval using a start frame 
    and an ending frame buffer, then assigns these intervals to a class. The final output is a CSV file ("dataset.csv") 
    that aggregates each video's filename, class label, begin frame, and end frame into different classes based on time.

    This script is meant to run to test how much a model can infer based on the time period that the dataset is run

Usage:
    Run the script from the command line in the following format:
        python time_based_division.py [--path PATH] [--counts COUNTS_CSV]
                                      [--splits SPLITS] [--start-frame START_FRAME]
                                      [--end-frame-buffer END_FRAME_BUFFER]

Arguments:
    --path:
        The directory path where the counts CSV file is located.
        (Default: ".")
    
    --counts:
        The filename of the counts CSV file. The file is expected to contain at least 'filename' and 'framecount' columns.
        (Default: "counts.csv")
    
    --splits:
        The number of splits (or classes) to create from the total number of videos. The script divides the videos equally 
        among these classes.
        (Default: 3)
    
    --start-frame:
        The starting frame number used for determining the valid frame interval for each video.
        (Default: 0)
    
    --end-frame-buffer:
        The number of frames to subtract from the video's total frame count to determine the end frame of the interval.
        (Default: 0)

Workflow:
    1. The script sets up logging and parses the command line arguments.
    2. It reads the counts CSV file from the specified directory.
    3. It calculates how many videos belong to each class by performing an integer division of the total number of videos 
       by the provided split count.
    4. For each video, it determines the beginning and ending frames. The beginning frame is the smaller value between 
       the video's frame count and the specified start frame, ensuring validity. The end frame is computed by subtracting 
       the end frame buffer from the video's frame count.
    5. It populates a DataFrame with the video's filename, assigned class (based on its split), and the calculated frame 
       interval.
    6. The final DataFrame is then saved as "dataset.csv".
    7. Throughout the process, key steps and information are logged for monitoring purposes.

Dependencies:
    - pandas: Used for reading, manipulating, and writing CSV data.
    - logging: Provides logging capabilities to monitor script execution and key actions.
    - argparse: Facilitates command line argument parsing.
    - os: Handles file path operations.
"""


import pandas as pd
import logging
import argparse
import os

if __name__ == "__main__":

    logging.basicConfig(
        format="%(asctime)s: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Splitting up a counts.csv into a dataset.csv that has time based class divisions"
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
        "--splits",
        type=int,
        help="number of splits per video, default 3",
        default=3,
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
    args = parser.parse_args()

    logging.info("Running the Dataset_Creator/time_based_division.py script")
    logging.info("Finding Dataset files")

    logging.info(
        "ARGUMENTS\n"
        f"the path to the counts.csv file: {args.counts}\n"
        f"the number of splits made in the data: {args.splits}\n"
        f"the start frame for the function: {args.start_frame}\n"
        f"the buffer for the end frame: {args.end_frame_buffer}\n"
    )

    counts = pd.read_csv(os.path.join(args.path, args.counts))
    number_of_videos = len(counts.index)
    videos_per_class = number_of_videos // args.splits
    logging.info(
        f"Read counts.csv, found {number_of_videos} videos, each class will have {videos_per_class} videos"
    )

    final_dataframe = pd.DataFrame(
        columns=["filename", "class", "beginframe", "endframe"]
    )

    class_count = 0

    for class_idx in range(args.splits):
        # for each video, update the final dataframe
        for video_idx in range(videos_per_class):
            video_framecount = counts.iloc[
                class_idx * videos_per_class + video_idx
            ].framecount
            begin_frame = min(
                video_framecount, args.start_frame
            )  # make sure that the begin frame is valid
            end_frame = video_framecount - args.end_frame_buffer

            if begin_frame > end_frame:
                # we're not going backwards lmao
                continue

            final_dataframe.loc[class_idx * videos_per_class + video_idx] = [
                counts.iloc[class_idx * videos_per_class + video_idx].filename,
                class_idx,
                begin_frame,
                end_frame,
            ]

    final_dataframe.to_csv("dataset.csv")
    logging.info("Completed creating the dataset.csv")
