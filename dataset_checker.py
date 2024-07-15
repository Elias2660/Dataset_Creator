import pandas as pd
import subprocess
import re
import logging

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
logging.info("Finding Dataset files")
command = "ls dataset_*.csv"
output = subprocess.run(command, shell=True, capture_output=True, text=True)
ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
file_list = sorted(
    [ansi_escape.sub("", line) for line in output.stdout.splitlines()]
)
logging.info(f"found dataset files: {file_list}")

def check_dataset(path:str):
    dataset = pd.read_csv(path)
    faulty_rows = []
    for i in range(len(dataset)):
        if dataset.iloc[i].isnull().values.any():
            logging.error(f"Found Error Row: {dataset.iloc[i]}".replace("\n", " "))
            logging.error(f"Dataset has missing values at row {i}")
            faulty_rows.append(i)
        elif int(dataset.iloc[i, 2 ]) >= int(dataset.iloc[i, 3]):
            logging.error(f"Found Error Row: {dataset.iloc[i]}".replace("\n", " "))
            logging.error(f"Dataset has begin frame greater than or equal to end frame at row {i}")
            faulty_rows.append(i)
        elif int(dataset.iloc[i, 2 ]) < 0 or int(dataset.iloc[i, 3]) < 0:
            logging.error(f"Found Error Row: {dataset.iloc[i]}".replace("\n", " "))
            logging.error(f"Dataset has begin frame or end frame less than or equal to 0 at row {i}")
            faulty_rows.append(i)
    logging.info(f"Found {len(faulty_rows)} faulty rows")
    
    if len(faulty_rows) == 0:
        logging.info(f"Since no faulty rows have been found, dataset is clean and no backups will be made")
        return
    logging.info(f"Cleaning dataset")
    for i in faulty_rows:
        dataset.drop(i, inplace=True)
    
    dataset.reset_index(drop=True, inplace=True)
    logging.info(f"Dataset has been cleaned, moving old dataset to backup")
    subprocess.run(f"mv {path} {path}.bak", shell=True)
    logging.info(f"Saving cleaned dataset to {path}")
    dataset.to_csv(path, index=False)

for file in file_list:
    check_dataset(file)