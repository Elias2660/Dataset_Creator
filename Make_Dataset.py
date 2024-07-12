# %%

import pandas as pd
import numpy as np
import subprocess
import logging
import argparse

# %%

# Load the data

# parser = argparse.ArgumentParser(description='Create Dataset file')

# parser.add_argument('--input', type=str, help='Input file path')
counts = pd.read_csv("counts.csv")
logNo = pd.read_csv("logNo.txt", names=["frame_name"])
logPos = pd.read_csv("logPos.txt", names=["frame_name"])
logNeg = pd.read_csv("logNeg.txt", names=["frame_name"])
# %%

"""
the format of the new processed dataframe would be 
1. time, name, class, start frame, end frame, 
"""


# Add time data to the counts.csv


def process_frame_count(counts: pd.DataFrame):
    processed_counts = pd.DataFrame()
    processed_counts["time"] = pd.to_datetime(
        counts["Filename"].str.replace(".h264", "").replace(".mp4", ""),
        format="%Y-%m-%d %H:%M:%S.%f",
    )
    processed_counts["filename"] = counts["Filename"]
    processed_counts["class"] = np.nan
    processed_counts["start frame"] = 0
    processed_counts["end frame"] = np.nan
    return processed_counts


processed_counts = process_frame_count(counts)

# %%


def process_log_files(log: pd.DataFrame, classNum: int):
    processed_log = pd.DataFrame()
    processed_log["time"] = pd.to_datetime(log["frame_name"], format="%Y%m%d_%H%M%S")
    processed_log["filename"] = np.nan
    processed_log["class"] = classNum
    processed_log["start frame"] = np.nan
    processed_log["end frame"] = np.nan
    return processed_log


processed_logNeg = process_log_files(logNeg, 0)
processed_logNo = process_log_files(logNo, 1)
processed_logPos = process_log_files(logPos, 2)

# %%

FPS = 25


LOG_NEG_CLASS_VALUE = 0
LOG_NO_CLASS_VALUE = 1
LOG_POS_CLASS_VALUE = 2

def create_dataset(frame_counts: pd.DataFrame, processed_counts: pd.DataFrame, *args):

    dataset = pd.concat([processed_counts, *args], ignore_index=True)
    dataset = dataset.sort_values(by="time").reset_index(drop=True)
    # for filenames




    dataset["filename"] = dataset["filename"].ffill()
    dataset = dataset.dropna(subset=["filename"]).reset_index(drop=True)
    print(frame_counts.columns)
    # for frames
    for i in range(len(dataset)):
        if i == len(dataset) - 1:
            row_value = frame_counts.loc[
                frame_counts["Filename"] == dataset.loc[i, "filename"], "Frame count"]
            dataset.loc[i, "end frame"] = row_value.values[0]
            dataset.loc[i, "start frame"] = dataset.loc[i-1, "end frame"] + 1
        elif np.isnan(dataset.loc[i, "start frame"]) and np.isnan(
            dataset.loc[i, "end frame"]
        ):
            dataset.loc[i, "start frame"] = dataset.loc[i - 1, "end frame"] + 1
            dataset.loc[i, "end frame"] = dataset.loc[i, "start frame"] + round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
            )
        elif dataset.loc[i + 1, "start frame"] == 0:
            row_value = frame_counts[
                frame_counts["Filename"] == dataset.loc[i, "filename"]
            ]["Filename"]
            dataset.loc[i, "end frame"] = row_value
        elif i == 0 and np.isnan(dataset.loc[i, "end frame"]):
            dataset.loc[i, "end frame"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
            )

        elif dataset.loc[i, "start frame"] == 0 and np.isnan(dataset.loc[i, "end frame"]):
            dataset.loc[i, "end frame"] = round(
                (dataset.loc[i + 1, "time"] - dataset.loc[i, "time"]).seconds * FPS
            )
            
        # for classes
        if np.isnan(dataset.loc[i, "class"]) and i == 0:
            dataset.loc[i, "class"] = LOG_NO_CLASS_VALUE
        elif np.isnan(dataset.loc[i, "class"]):
            dataset.loc[i, "class"] = dataset.loc[i-1, "class"]
        


    # for end frames
    dataset["class"] = dataset["class"].astype(int)
    dataset["start frame"] = dataset["start frame"].astype(int)
    dataset["end frame"] = dataset["end frame"].astype(int)
    return dataset


dset = create_dataset(
    counts, processed_counts, processed_logNeg, processed_logNo, processed_logPos
)
print(dset)
dset.to_csv("dataset.csv", index=False)
# %%
