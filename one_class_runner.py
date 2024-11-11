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
        "--path", type=str, help="path to where directory is located", default=".", required=False
    )
    parser.add_argument(
        "--counts", type=str, help="path to counts file, default is counts.csv", default="counts.csv", required=False
    )
    parser.add_argument(
        "--start-frame", type=int, help="start frame, default zero", default=0, required=False
        )
    parser.add_argument(
        "--end-frame-buffer", type=int, help="the end frame that one would use, default 0", default=0, required=False
        )
    parser.add_argument(
        "--splits", type=int, help="number of splits per video, default 4", default=4, required=False
    )
    args = parser.parse_args()
    logging.info(f"Arguments: path={args.path}, counts={args.counts}, start_frame={args.start_frame}, end_frame_buffer={args.end_frame_buffer}, splits={args.splits}")
    
    counts = pd.read_csv(os.path.join(args.path, args.counts))
    
    final_dataframe = pd.DataFrame(columns=["filename", "class", "beginframe", "endframe"])
    class_count = 0
    
    for row in counts.iterrows():
        frame_interval = (row[1]["framecount"] - args.end_frame_buffer - args.start_frame) // args.splits
        begin_frame = args.start_frame
        end_frame = row[1]["framecount"] - args.end_frame_buffer
        for split in range(args.splits):
            final_dataframe = pd.concat([final_dataframe, pd.DataFrame([{"filename": row[1]["filename"], "class": class_count, "beginframe": begin_frame, "endframe": end_frame}])], ignore_index=True)
            begin_frame += frame_interval
            end_frame += frame_interval

        class_count += 1
    
    final_dataframe.to_csv(os.path.join(args.path, "dataset.csv"), index=False)
    
    
        