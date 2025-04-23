import argparse
import json
import snappy
import os
import re
import logging
import threading
import queue

UUID_JSON_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$")
WORKER_COUNT = 4

def setup_logging(verbose, log_file):
    logger = logging.getLogger("snappy_decoder")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler = logging.FileHandler(log_file) if log_file else logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s"))
    logger.addHandler(handler)
    return logger

def find_matching_files(directory):
    matching_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if UUID_JSON_PATTERN.match(file):
                matching_files.append(os.path.join(root, file))
    return matching_files

def worker(task_queue, result_queue, logger, verbose):
    while True:
        input_task = task_queue.get()
        if input_task is None:
            task_queue.task_done()  # Ensure task completion is marked before exiting
            break  # Exit the thread when sentinel value is received
        
        input_file, output_file = input_task
        json_objects = decode_snappy_file(input_file, output_file, logger, verbose)
        result_queue.put(json_objects)
        task_queue.task_done()

def decode_snappy_file(input_file, output_file, logger, verbose):
    try:
        with open(input_file, 'rb') as f:
            header_line = f.readline().decode('utf-8').strip()
            try:
                header = json.loads(header_line)
            except json.JSONDecodeError:
                logger.warning(f"{input_file}: Invalid JSON header")
                return 0

            if header.get("compression") != "snappy":
                logger.warning(f"{input_file}: Unsupported compression type: {header.get('compression')}")
                return 0

            compressed_data = f.read()
            try:
                decompressed_data = snappy.uncompress(compressed_data)
            except snappy.UncompressError:
                logger.warning(f"{input_file}: Snappy decompression failed")
                return 0

        with open(output_file, 'wb') as out_f:
            out_f.write(decompressed_data)
        
        json_objects = decompressed_data.decode('utf-8').count('\n')
        if verbose:
            logger.info(f"{input_file}: Decompressed successfully with {json_objects} log entries")
        return json_objects
    
    except Exception as e:
        logger.error(f"Error processing {input_file}: {e}")
        return 0

def remove_uncompressed_files(directory, logger):
    to_remove = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith("_uncompress.json"):
                to_remove.append(os.path.join(root, file))
    
    if not to_remove:
        logger.info("No files found for removal.")
        return
    
    logger.info(f"Found {len(to_remove)} files for removal.")
    confirm = input(f"Do you want to delete {len(to_remove)} files? (y/N): ").strip().lower()
    if confirm != 'y':
        logger.info("Deletion aborted by user.")
        return
    
    deleted_count = 0
    for file in to_remove:
        try:
            os.remove(file)
            logger.debug(f"Deleted {file}")
            deleted_count += 1
        except Exception as e:
            logger.error(f"Failed to delete {file}: {e}")
    
    logger.info(f"Deleted {deleted_count} of {len(to_remove)} files.")

def main():
    parser = argparse.ArgumentParser(description="Decode Snappy compressed JSON files.")
    parser.add_argument('--input-file', help="Single input file to decompress.")
    parser.add_argument('--input-dir', help="Directory to search for compressed JSON files.")
    parser.add_argument('--verbose', '-v', action='store_true', help="Enable verbose output.")
    parser.add_argument('--log-file', help="Write logs to this file instead of stdout.")
    parser.add_argument('--remove', action='store_true', help="Remove all *_uncompress.json files.")
    
    args = parser.parse_args()
    
    logger = setup_logging(args.verbose, args.log_file)
    
    if args.input_file and args.input_dir:
        logger.error("Error: You must specify either --input-file or --input-dir, not both.")
        exit(1)
    
    if args.remove:
        if not args.input_dir:
            logger.error("Error: --remove requires --input-dir to be specified.")
            exit(1)
        remove_uncompressed_files(args.input_dir, logger)
        return
    
    total_json_objects = 0
    if args.input_file:
        output_file = args.input_file.replace(".json", "_uncompress.json")
        total_json_objects += decode_snappy_file(args.input_file, output_file, logger, args.verbose)
    
    elif args.input_dir:
        matching_files = find_matching_files(args.input_dir)
        logger.info(f"Found {len(matching_files)} matching files in {args.input_dir}.")
        
        task_queue = queue.Queue()
        result_queue = queue.Queue()
        
        for file in matching_files:
            output_file = file.replace(".json", "_uncompress.json")
            task_queue.put((file, output_file))
        
        threads = []
        for i in range(WORKER_COUNT):
            thread = threading.Thread(target=worker, args=(task_queue, result_queue, logger, args.verbose), name=f"Worker-{i+1}")
            thread.start()
            threads.append(thread)
        
        for _ in range(WORKER_COUNT):
            task_queue.put(None)
        
        task_queue.join()
        
        for thread in threads:
            thread.join()
        
        while not result_queue.empty():
            total_json_objects += result_queue.get()
        
        if not args.verbose:
            logger.info(f"Total log entries across all files: {total_json_objects}")
    
if __name__ == "__main__":
    main()

