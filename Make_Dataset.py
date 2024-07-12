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
logNo = pd.read_csv('logNo.txt', names=["year/month/day_hour/min/sec"])
logPos = pd.read_csv('logPos.txt', names=["year/month/day_hour/min/sec"])
logNeg = pd.read_csv('logNeg.txt', names=["year/month/day_hour/min/sec"])
# %%

"""
the format of the new processed dataframe would be 
1. time, name, class, start frame, end frame, 
"""

# Add time data to the counts.csv

def process_frame_count(counts: pd.DataFrame):
    processed_counts = pd.DataFrame()
    processed_counts["time"] = pd.to_datetime(counts["Filename"].str.replace(".h264", "").replace(".mp4", ""), format="%Y-%m-%d %H:%M:%S.%f")
    processed_counts["Filename"] = counts["Filename"]
    processed_counts["class"] = "NaN"
    processed_counts["start frame"] = "NaN"
    processed_counts["end frame"] = counts["Frame count"]
    return processed_counts
    
processed_counts = process_frame_count(counts)

# %%

def process_log_files(log:pd.DataFrame):
    processed_log = pd.DataFrame()
    