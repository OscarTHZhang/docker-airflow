
import argparse
import csv
import logging
import os.path
import sys
import subprocess
import pdb
import ntpath
import uuid

__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"


logger = logging.getLogger(__name__)


# def fix_animal_file(filename):
#     subprocess.call(["sed", "$d", filename], stdout=open(filename + ".fixed", "w"))
#     return filename + ".fixed"



def fix_animal_file(in_filename):
    num_columns = 0
    filename, file_extension = os.path.splitext(in_filename)
    out_filename = "/tmp/" + ntpath.basename(filename) + str(uuid.uuid4())
    with open(out_filename, "w+") as out_csv:
        csv_writer = csv.writer(out_csv, delimiter=',')
        with open(in_filename) as in_csv:  
            csv_reader = csv.reader(in_csv, delimiter=',')
            num_columns = 0
            for row in csv_reader:
                row.pop()
                if row[0][0:5] != 'Total':            
                    if num_columns == 0:
                        num_columns = len(row)
                        logger.debug("Set row count to: " + str(num_columns))
                    elif len(row) > num_columns:
                        logger.debug("unshrunk row: " + str(row))
                        shrunk_row = shrink_animal_row(row, num_columns)
                        logger.debug("shrunk row: " + str(shrunk_row))
                    elif len(row) < num_columns:
                        logger.error("Row has too few columns!")
                        logger.error("row = " + str(row))
                        exit(1)    
                    for pos in range(len(row)):
                        row[pos] = row[pos].strip()
                    logger.debug("writing row: " + str(row))
                    csv_writer.writerow(row)
    return out_filename




def shrink_animal_row(row, num_columns):
    # how many extra rows?
    extra_row_count = len(row) - num_columns
    if extra_row_count > 0:
        remark = row[15] + " "
        for i in range(extra_row_count):
            remark += row.pop(16)
        row[8] = remark
    return row



def fix_event_file(in_filename):
    num_columns = 0
    filename, file_extension = os.path.splitext(in_filename)
    # out_filename = filename + file_extension + ".fixed"
    out_filename = "/tmp/" + ntpath.basename(filename) + "-" + str(uuid.uuid4())
    with open(out_filename, "w+") as out_csv:
        csv_writer = csv.writer(out_csv, delimiter=',')
        with open(in_filename) as in_csv:  
            csv_reader = csv.reader(in_csv, delimiter=',')
            num_columns = 0
            for row in csv_reader:
                row.pop()
                if num_columns == 0:
                    num_columns = len(row)
                    logger.debug("Set row count to: " + str(num_columns))
                elif len(row) > num_columns:
                    logger.debug("unshrunk row: " + str(row))
                    shrunk_row = shrink_row(row, num_columns)
                    logger.debug("shrunk row: " + str(shrunk_row))
                elif len(row) < num_columns:
                    logger.error("Row has too few columns!")
                    logger.error("row = " + str(row))
                    exit(1)
                for pos in range(len(row)):
                    row[pos] = row[pos].strip()
                logger.debug("writing row: " + str(row))
                csv_writer.writerow(row)
    return out_filename





def shrink_row(row, num_columns):
    # how many extra rows?
    extra_row_count = len(row) - num_columns
    if extra_row_count > 0:
        remark = row[8] + " "
        for i in range(extra_row_count):
            remark += row.pop(9)
        row[8] = remark
    return row

    
