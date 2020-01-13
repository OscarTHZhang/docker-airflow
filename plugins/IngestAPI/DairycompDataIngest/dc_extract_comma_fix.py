""" Basic CLI to take output files produced from the dairy comp 305 software program
    which contain farm event data and process them in a way that concatenates comma-separated 
    entries to the remarks field, strips leading and trailing whitespaces from all fields, 
    removes a seemingly useless empty last column, and outputs the data in a new csv with 
    the same name as the original, but appended w/ '_fixed'. 

    Note that this script is currently hardcoded that the remarks column exists as the 9th 
    column in the input file - any other use will require slight modification.

    This was constructed using python 2.7 and has a basic commandline interface which can 
    be used to specify one or more input files, along with a verbosity level.
 """

import csv
import logging
import os.path


__author__ = "Steven Wangen, Oscar Zhang"
__version__ = "1.0.2"
__email__ = "srwangen@wisc.edu, tzhang383@wisc.edu"
__status__ = "Development"


logger = logging.getLogger(__name__)


def data_ingest(file_directory):
    # parse files
    # determine files
    filenames = []
    if not os.path.isdir(file_directory):
        filenames.append(file_directory)
    elif os.path.isdir(file_directory):
        for fn in os.path(file_directory):
            filenames.append(fn)

    for filename in filenames:
        logging.info("parsing files: " + str(filename))
        if os.path.exists(filename):
            parse_file(filename)
        else:
            logging.error('File ' + filename + ' not found!')


def parse_file(in_filename):
    filename, file_extension = os.path.splitext(in_filename)
    out_filename = filename + "_fixed" + file_extension
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


def shrink_row(row, num_columns):
    # how many extra rows?
    extra_row_count = len(row) - num_columns
    if extra_row_count > 0:
        remark = row[8] + " "
        for i in range(extra_row_count):
            remark += row.pop(9)
        row[8] = remark
    return row