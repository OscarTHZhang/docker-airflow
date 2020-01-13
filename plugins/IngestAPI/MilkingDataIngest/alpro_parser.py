"""
Parsing and importing dry matter intake files from DeLaval AlPro software
into a database or csv
"""

import pandas as pd
import datetime
import logging
from sqlalchemy.sql import text


__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"


logger = logging.getLogger(__name__)


def parse_file(test, farm_id, filename, db_engine):
    # set file encoding (dunno why no utf-8 - legacy?)
    file_encoding = "ISO-8859-1"

    # get date info from file
    date_df = pd.read_csv(filename, sep=",", encoding=file_encoding, header=None, nrows=1)
    
    # extract the report date
    report_date = extract_date_from_cell(str(date_df[2]))

    # dump raw input into df
    input_df = pd.read_csv(filename, sep=",", encoding=file_encoding, header=3)

    # get the number of columns
    column_count = input_df.shape[1] - 1

    # create a df as a function of that number
    num_milkings = get_num_milkings(column_count)
    # milking_df = make_milking_df(num_milkings)
    
    # transform the df into a structure w/ 1 milking observation per row
    transformed_milking_df = transform_alpro_df(input_df, farm_id, report_date, num_milkings)

    if test:
        # write to csv
        transformed_milking_df.to_csv(path_or_buf='test_output.csv')

    else:
        # create a temp file in the database
        create_milking_table(db_engine)

        # write df to temp table
        write_milk_to_db(transformed_milking_df, db_engine)
        
        # log success messsage
        logger.info('AlPro data file ' + filename + ' successfully written to database.')
   

def transform_alpro_df(input_df, farm_id, report_date, num_milkings):

    # create a dataframe w/ 1 milking per row
    db_column_names = ['farm_id', 'date', 'cow_no', 'group_no', 'DIM', 'milk', 'peak_flow', 'avg_flow', 'duration', 'stall_position']
    milking_df = pd.DataFrame(columns=db_column_names)

    # adjust date (because AlPro columns suggest this is *yesterday's* data)
    milking_date = report_date - datetime.timedelta(days=1)

    # transcribe the rows
    for row in input_df.iterrows():
        for i in range(num_milkings):
            # create row
            new_row = [farm_id, milking_date, row[1][0], row[1][1], row[1][2], row[1][i + 4], row[1][i + 6], row[1][i + 8], row[1][i + 10], row[1][i + 12]]

            # append row to df
            milking_df.loc[len(milking_df)] = new_row

    return milking_df


def get_num_milkings(column_count):
    # count the number of fixed columns - 'Cow No', 'Group No', 'Days In Milk', 'Tot. Milk Yest'
    num_fixed_columns = 4

    # figure out number of milking columns
    num_milk_columns = column_count - num_fixed_columns

    # make sure it's divisible by 5
    # (number of per-milking attributes - 'Milk  Yest', 'Peak Flow Yest', 'Avg Flow', 'Milk Dur', 'Stall position'
    if num_milk_columns % 5 != 0:
        logging.error("alpro_parser.make_milking_df() - number of milking columns not divisible by 5!")
        exit(1)
    else:
        num_milkings = num_milk_columns / 5

    return num_milkings


def make_milking_df(num_milkings):

    # create write_df
    db_column_names = ['Cow No', 'Group No', 'Days In Milk', 'Tot. Milk Yest']
    
    for i in range(num_milkings):
        db_column_names.append('Milk Yest  %d' % (i + 1))
        db_column_names.append('Peak Flow Yest  %d' % (i + 1))
        db_column_names.append('Avg Flow Yest  %d' % (i + 1))
        db_column_names.append('Milk Dur Yest  %d' % (i + 1))
        db_column_names.append('Stall Position Yest  %d' % (i + 1))

    logging.debug("columns in first df: " + str(db_column_names))

    milking_df = pd.DataFrame(columns=db_column_names)
    return milking_df


def extract_date_from_cell(date_cell):
    date_str = (date_cell.split('Date: '))[1].split(' Time')[0]
    date = datetime.datetime.strptime(date_str, "%m/%d/%y")
    return date


def write_milk_to_db(write_df, db_engine):

    table_name = "alpro_milking_data"

    insert_statement = text("""INSERT INTO alpro_milking_data (farm_id, cow_number, group_number, milk_date, dim, milk, peak_flow, avg_flow, duration, stall_position) VALUES (:farm_id, :cow_number, :group_number, :milk_date, :dim, :milk, :peak_flow, :avg_flow, :duration, :stall_position)""")

    with db_engine.connect() as con:
        for row in write_df.itertuples():

            if record_exists(row, con):
                logger.info("duplicate record found in alpro_milking_table - skipping this entry!")

            else:
                data = ({
                    "farm_id": row.farm_id, "cow_number":row.cow_no,
                    "group_number": row.group_no,
                    "milk_date": row.date.strftime("%m/%d/%y"), 
                    "dim": row.DIM,
                    "milk": get_float(row.milk),
                    "peak_flow": get_float(row.peak_flow),
                    "avg_flow": get_float(row.avg_flow),
                    "duration": get_sec(row.duration), 
                    "stall_position": get_int(row.stall_position)
                    })
                
                try:
                    # pdb.set_trace()
                    con.execute(insert_statement, data)    
                    logger.info("Inserting to temp_milking_table...")
                    logger.debug('insert to temp_milking statement = ' + str(insert_statement))
                except Exception as e:
                    logger.error("alpro_parser.py - write_milk_to_db(): Error inserting to alpro_milking_data!")
                    logger.error(e, e.args)
                    exit(1)


def record_exists(row, con):
    logging.debug('checking if row already exists')

    statement = text("""SELECT count(*) from alpro_milking_data WHERE cow_number = :cow_number AND group_number = :group_number AND milk_date = :milk_date AND dim = :DIM
        AND milk = :milk AND peak_flow = :peak_flow AND avg_flow = :avg_flow AND duration = :duration AND stall_position = :stall_position""")

    data = ({
        "cow_number":row.cow_no, 
        "group_number":row.group_no, 
        "milk_date": row.date.strftime("%m/%d/%y"), 
        "DIM":row.DIM, 
        "milk":get_float(row.milk),
        "peak_flow":get_float(row.peak_flow),
        "avg_flow":get_float(row.avg_flow), 
        "duration": get_sec(row.duration), 
        "stall_position":get_int(row.stall_position)
        })
    
    result_set = con.execute(statement, data)
    
    if result_set.fetchone()[0] > 0:
        logging.debug('duplicate record found in db - return true value')
        return True
    else:
        logging.debug('no duplicate record found in db - return false value')


def get_float(value):
    if value.lstrip() == '-':
        return None
    else:
        return float(value.encode('ascii'))


def get_int(value):
    if value.lstrip() == '-':
        return None
    else:
        return int(value.encode('ascii'))


def get_sec(time_str):
    if time_str.lstrip() == '-':
        return None
    else:
        m, s = time_str.encode('ascii').split(':')
        return int(m) * 60 + int(s)


def create_milking_table(db_engine):
    
    table_name = "alpro_milking_data"
    
    # check and delete if table already exists
    if not db_engine.dialect.has_table(db_engine, table_name):
        
        # create the new table
        create_table_statement = text( \
            'CREATE TABLE ' + table_name +' ( '\
            'id                  serial primary key,'\
            'farm_id             bigint,'\
            'cow_number          bigint,'\
            'group_number        bigint,'\
            'milk_date           date,'\
            'dim                 bigint,'\
            'milk                float,'\
            'peak_flow           float,'\
            'avg_flow            float,'\
            'duration            bigint,'\
            'stall_position      bigint);'\
            )

        with db_engine.connect() as con:
            try:
                logger.info("Creating alpro_table...")
                logger.debug('create_milking_table statement = ' + str(create_table_statement))
                con.execute(create_table_statement)   
            except Exception as e:
                logger.error("Error creating the alpro_milking_table in database!")
                logger.error(e)
                exit(1)


def get_a_float(input):
    if type(input) == float:
        return input
    elif type(input) is str:
        return float(input.replace(',', ''))
