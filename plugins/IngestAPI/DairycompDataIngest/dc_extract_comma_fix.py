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


import argparse
import csv
import logging
import os.path


__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"


logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Read export files from DairyComp software and remove extraneous commas in remarks.')
    parser.add_argument("-v", "--verbose", action='store_true',
                    help="increase verbosity to include debug messages.")
    parser.add_argument('filenames', metavar='filenames', type=str, nargs='*',
                    help='name of file(s) to be processed')
    
    args = parser.parse_args()
    main(args)


def main(args):
    # set logging level
    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.WARN
    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(levelname)s %(message)s')

    logging.debug('args = ' + str(args))
    
    # parse files 
    for filename in args.filenames:
        logging.info("parsing files: " + str(filename))
        if os.path.exists(filename):
            parse_file(filename)
        else:
            logging.error('File ' + filename + ' not found!')


def parse_file(in_filename):
    num_columns = 0
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


if __name__ == "__main__":
    # main(sys.argv)
    parse_args()

