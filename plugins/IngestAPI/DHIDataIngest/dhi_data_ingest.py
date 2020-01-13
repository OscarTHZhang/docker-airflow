""" Wrapper bit to implement parsing and importing dry matter intake
    files from feed management software into a database or file """

import os.path
import logging

import ntpath
from IngestAPI.DHIDataIngest import agsource_parser
from configs.getEngine import get_db_engine

__author__ = "Steven Wangen,Oscar Zhang"
__version__ = "0.2"
__email__ = "srwangen@wisc.edu, tzhang383@wisc.edu"
__status__ = "Development"

logger = logging.getLogger(__name__)


def populate_file_list(directory, input_files):
    # swiped from https://stackoverflow.com/questions/9816816/get-absolute-paths-of-all-files-in-a-directory
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            input_files.append(os.path.abspath(os.path.join(dirpath, f)))


def data_ingest(file_directory, is_testing, farm_id, db_log=True):

    # set farm id
    if farm_id is None:
        logging.error("Farm id not specified!")
        exit(1)

    # test mode
    if is_testing:
        logging.info('Running in test mode - output local csv');
    # if len(args.filenames) == 0:       
    #     logging.info('No filenames specified - using local test_input.csv')
    #     args.filenames = ["test_input.csv"] 

    # create db engine, pass db_logging boolean
    db_engine = None
    if not is_testing:
        db_engine = get_db_engine(db_log)

    # determine files 
    path = file_directory
    filenames = []
    populate_file_list(path, filenames)

    spectrum_results = []
    component_results = []

    logging.info("parsing files: " + str(filenames))

    for fn in filenames:
        # make sure the file exists
        if os.path.exists(fn):
            # choose appropriate parsing library
            logging.info("parsing file " + fn)
            # if args.datasource_vendor_shortname[0].lower() == 'dhi':
            # else:
            #    logging.error('Datasource_vendor_shortname (%s) not recognized - skipping!',
            # (args.datasource_vendor_shortname[0]))
            # id and log the file type ('components' vs 'SpectrumData')

            # decode file type from filename
            min_file_name = ntpath.basename(fn)

            if 'components' in min_file_name.lower():
                result = agsource_parser.parse_components(is_testing, farm_id, fn, db_engine)
                component_results.append(result[1])
            elif 'spectrum' in min_file_name.lower():
                result = agsource_parser.parse_spectrum(is_testing, farm_id, fn, db_engine)
                spectrum_results.append(result[1])
            else:
                logging.warn("file " + fn + " not recoginized as valid DHI data source - skipping...")

        else:
            logging.error('File ' + fn + ' not found!')
            exit(1)

    logging.warn('Parsing complete - successfully read {}/{} component files and {}/{} spectrum files'.
                 format(component_results.count(True), len(component_results), spectrum_results.count(True),
                        len(spectrum_results)))
