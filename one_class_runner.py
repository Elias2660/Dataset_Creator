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
    logging.info("Finding Dataset files")

    parser = argparse.ArgumentParser(
        description="Special case for dataprep videos where each video is one different class"
    )
    parser.add_argument(
        "--path", type=str, help="path to where directory is located", default="."
    )
    parser.add_argument(
        "--counts", type=str, help="path to counts file, default is counts.csv", default="counts.csv"
    )
    parser.add_argument(
        "--start-frame", type=int, help="start frame, default zero", default=0
        )
    parser.add_argument(
        "--end-frame-buffer", type=int, help="the end frame that one would use, default 0", default=0
        )
    args = parser.parse_args()
    counts = pd.read_csv(os.path.join(args.path, args.counts))
    
    final_dataframe = pd.DataFrame(columns=["filename", "class", "beginframe", "endframe"])
    class_count = 0
    
    for row in counts.iterrows():
        filename = row[1]["filename"]
        class_name = row[1]["class"]
        framecount = row[1]["framecount"]
        begin_frame = args.start_frame
        end_frame = row["framecount"] - args.end_frame_buffer
        for i in range(framecount):
            final_dataframe = final_dataframe.append({"filename": filename, "class": class_name, "beginframe": begin_frame, "endframe": end_frame}, ignore_index=True)
            class_count += 1
    
    final_dataframe.to_csv(os.path.join(args.path, "dataset.csv"), index=False)
    
    
        