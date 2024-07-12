#%%

import pandas as pd
import numpy as np
import subprocess
import logging
import argparse
#%% 

# Load the data

# parser = argparse.ArgumentParser(description='Create Dataset file')

# parser.add_argument('--input', type=str, help='Input file path')
counts = pd.read_csv('counts.csv')
logNo = pd.read_csv('logNo.txt', names=["frame_name"])
logPos = pd.read_csv('logPos.txt', names=["frame_name"])
logNeg = pd.read_csv('logNeg.txt', names=["frame_name"])
# %%

"""
the format of the new processed dataframe would be 
1. time, name, class, start frame, end frame, 
"""



# Add time data to the counts.csv

def process_frame_count(counts: pd.DataFrame):
    processed_counts = pd.DataFrame()
    processed_counts["time"] = pd.to_datetime(counts["Filename"].str.replace(".h264", "").replace(".mp4", ""), format="%Y-%m-%d %H:%M:%S.%f")
    processed_counts["filename"] = counts["Filename"]
    processed_counts["class"] = np.nan
    processed_counts["start frame"] = np.nan
    processed_counts["end frame"] = counts["Frame count"]
    return processed_counts
    
processed_counts = process_frame_count(counts)

# %%

def process_log_files(log:pd.DataFrame, classNum:int):
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

def create_dataset(processed_counts: pd.DataFrame, *args):
    dataset = pd.concat([processed_counts, *args], ignore_index=True)
    dataset = dataset.sort_values(by="time").reset_index(drop=True)
    # for filenames
    
    dataset["filename"] = dataset["filename"].ffill()
    dataset = dataset.dropna(subset=["filename"]).reset_index(drop=True)
    
    # for classes
    
    
    # for start frames
    
    
    # for end frames
    
    
    return dataset
dset = create_dataset(processed_counts, processed_logNeg, processed_logNo, processed_logPos)
print(dset)
# %%

