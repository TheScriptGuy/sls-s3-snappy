

# Snappy JSON Decoder

This Python program is a multi-threaded utility for decoding Snappy-compressed JSON files. It can process a single file or recursively scan a directory for files matching a specific naming pattern.

---

## 🛠️ Requirements & Setup

Before running this program, you'll need to install the necessary dependencies.

### Dependencies

This program requires the `python-snappy` library.

### Installation

To install the required library, use `pip`:

```bash
pip install python-snappy
```

## 🚀 How to Use
The program can be used in three main modes, which are controlled by command-line arguments.

### 1. Decompress a Single File
To decompress a single Snappy-compressed JSON file, use the `--input-file` argument. The program will decompress the data and save it to a new file with `_uncompress.json` appended to the original filename.

```bash
$ python3 your_program_name.py --input-file 123e4567-e89b-12d3-a456-426614174000.json
```

### 2. Decompress All Files in a Directory
To process all files in a directory that match the UUID-based naming pattern (`XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX.json`), use the `--input-dir` argument. The program will recursively search the specified directory and its subdirectories.

```bash
$ python3 your_program_name.py --input-dir /path/to/your/files
```

### 3. Remove Decompressed Files
To clean up and remove all previously decompressed files (those ending with `_uncompress.json`), use the `--remove` argument in conjunction with `--input-dir`. Note: This action requires user confirmation before proceeding.

```bash
$ python3 your_program_name.py --input-dir /path/to/your/files --remove
```

#### Optional Arguments
* `--verbose` or `-v`: Enables verbose logging, providing detailed information about each file's decompression status and the number of log entries found.

* `--log-file`: Redirects all log output to a specified file instead of the standard output.

## 📝 Expected Output

### Successful Decompression

Upon successful decompression, a new file will be created in the same directory as the input file. For example, if the input is `123e4567-e89b-12d3-a456-426614174000.json`, the output file will be named `123e4567-e89b-12d3-a456-426614174000_uncompress.json`.

Without `--verbose`: The program will provide a summary of the total number of log entries processed.

```
INFO - Found 15 matching files in /path/to/your/files.
INFO - Total log entries across all files: 58760
With --verbose: The program will log the details for each file as it's processed, and also include the final summary.

INFO - [Worker-1] 123e4567-e89b-12d3-a456-426614174000.json: Decompressed successfully with 3560 log entries
INFO - [Worker-2] 123e4567-e89b-12d3-a456-426614174001.json: Decompressed successfully with 2251 log entries
...
INFO - Total log entries across all files: 58760
```

### Error & Warning Handling
The program will handle various issues gracefully and log a warning or error, depending on the severity.

Invalid JSON Header: If the first line of the file isn't a valid JSON object, a warning will be logged.

```
WARNING - 123e4567-e89b-12d3-a456-426614174002.json: Invalid JSON header
```

Unsupported Compression: If the JSON header specifies a compression type other than "snappy," a warning will be logged.

```
WARNING - 123e4567-e89b-12d3-a456-426614174003.json: Unsupported compression type: gzip
```
Decompression Failure: If the Snappy library fails to decompress the data (e.g., due to file corruption), a warning will be logged.
```
WARNING - 123e4567-e89b-12d3-a456-426614174004.json: Snappy decompression failed
```