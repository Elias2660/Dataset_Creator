# Dataset Creator (for the Rutgers Bee Project)

A set of command‑line tools for processing video frame counts and time‑stamped log files into clean, labeled dataset CSVs.

This project is most used with the [Unified‑bee‑Runner](https://github.com/Elias2660/Unified-bee-Runner).

---

## Introduction

These scripts help you:

- **Make_Dataset.py**: Merge a `counts.csv` of video frame counts with any number of timestamped log files (e.g. `logNo.txt`, `logPos.txt`, `logNeg.txt`) into one master `dataset.csv`, computing begin/end frames based on FPS, offsets, intervals, and buffers.
- **one_class_runner.py**: Treat each video as its own “class” and partition its usable frames into a specified number of splits, producing a master `dataset.csv` plus one split file per segment (e.g. `dataset_0.csv`, `dataset_1.csv`, …).
- **dataset_checker.py**: Validate and clean any dataset CSVs by checking for missing values, out‑of‑bounds frames, invalid ordering, and negatives—logging errors, backing up originals, and dropping faulty rows.

---

## Installation

Install Python dependencies:

```bash
pip install -r requirements.txt
```

> Requirements: pandas, numpy, argparse, logging, plus utils for FPS detection and standard library modules.

## Usage

All scripts support `--in-path` (input directory) and `--out-path` (output directory) arguments. If you omit `--in-path`, it defaults to `.`; `--out-path` is required for scripts that write files.

### 1. Generate Combined Dataset

```bash
python Make_Dataset.py \
  --in-path ./data \
  --out-path ./output \
  --counts_file counts.csv \
  --files logNo.txt,logPos.txt,logNeg.txt \
  --fps 25 \
  --starting-frame 1 \
  --frame-interval 0 \
  --end-frame-buffer 0
```
- Output:

    - `output/dataset.csv`

    - `output/RUN_DESCRIPTION.log` (maps each log file to its class number)

- Then: Automatically runs `dataset_checker.py` on the new `dataset.csv`.

### 2. Split by Class

```bash
python one_class_runner.py \
  --in-path ./data \
  --out-path ./output \
  --counts counts.csv \
  --start-frame 0 \
  --end-frame-buffer 0 \
  --splits 3

```

- Output:

    - output/dataset.csv (master list of all intervals)

    - output/dataset_0.csv, dataset_1.csv, dataset_2.csv (one random interval per class each)

### 3. Validate & Clean

```bash
python dataset_checker.py \
  --in-path ./output \
  --search-string "dataset_*.csv" \
  --counts counts.csv
```

- Backs up any cleaned files as `*.bak`
- Logs all checks at INFO level, drops invalid rows, and overwrites with cleaned CSVs.


## Contributing

[Contributions](CONTRIBUTING.md) are welcome! Please follow the guidelines in SECURITY.md and ensure compliance with the project's license.

## License

This project is licensed under the [MIT License](LICENSE).

## Security

Please review our [Security Policy](SECURITY.md) for guidelines on reporting vulnerabilities.



