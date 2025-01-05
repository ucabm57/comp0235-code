import sys
from subprocess import Popen, PIPE
import glob
import subprocess
import multiprocessing
import logging

# Set up logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def run_parser(input_file):
    search_file = input_file+"_search.tsv"
    logging.info(f'Running parser on {search_file}')
    cmd = ['python', '/home/almalinux/pipeline/results_parser.py', search_file]
    logging.info(f'STEP 2: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    logging.info(out.decode("utf-8"))
    if err:
        logging.error(err.decode("utf-8"))

def move_files(input_file):
    output_dir = "/home/almalinux/data/output/"
    files_to_move = [f"{input_file}.parsed", f"{input_file}_search.tsv", f"{input_file}_segment.tsv"]
    for file in files_to_move:
        result = subprocess.run(["mv", file, output_dir],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            logging.info(f"Moved {file} to {output_dir}")
        else:
            logging.error(f"Failed to move {file}: {result.stderr}")

def run_merizo_search(input_file, id):
    cmd = ['python3',
           '/home/almalinux/merizo_search/merizo_search/merizo.py',
           'easy-search',
           input_file,
           '/home/almalinux/data/cath-4.3-foldclassdb/cath-4.3-foldclassdb',
           id,
           'tmp',
           '--iterate',
           '--output_headers',
           '-d',
           'cpu',
           '--threads',
           '1'
           ]
    logging.info(f'STEP 1: RUNNING MERIZO: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    logging.info(out.decode("utf-8"))
    if err:
        logging.error(err.decode("utf-8"))

def read_dir(input_dir):
    logging.info(f'Reading directory {input_dir}')
    file_ids = list(glob.glob(input_dir+"*.pdb"))
    analysis_files = []
    for file in file_ids:
        id = file.rsplit('/', 1)[-1].split('.')[0]
        analysis_files.append([file, id])
    return(analysis_files)

def pipeline(filepath, id):4
    run_merizo_search(filepath, id)
    run_parser(id)
    move_files(id)
    logging.info(f'-------------Finished processing {id}-----------------')

if __name__ == "__main__":
    pdbfiles = read_dir(sys.argv[1])
    p = multiprocessing.Pool(4)
    p.starmap(pipeline, pdbfiles)