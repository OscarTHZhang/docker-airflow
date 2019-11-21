#!/usr/bin/env python
""" Parsing and importing dry matter intake files from feedwatch software
    into a database or csv """

import pandas as pd
import datetime
import logging
import uuid
import os
from sqlalchemy.sql import text

__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"

logger = logging.getLogger(__name__)


def parse_file(test, farm_id, filename, db_engine):
    # set file encoding (dunno why no utf-8 - legacy?)
    file_encoding = "ISO-8859-1"

    # dump raw input into df
    input_df = pd.read_csv(filename, sep=",", encoding=file_encoding)

    # create a uuid for a unique temp table
    uuid_str = uuid.uuid4().hex

    # id and log the file type (ingredient vs dmi)
    feedwatch_file_type = str(input_df.loc[0][1])
    logging.debug('feedwatch_file_type = ' + feedwatch_file_type)

    if feedwatch_file_type == 'DM Intake':

        # get the data from the file into a dataframe
        dmi_df = import_dmi_from_file(input_df, farm_id)

        if test:
            # find a csv name that doesn't exist
            i = 1
            csv_path = "test/test_output_" + str(i) + ".csv"
            while os.path.exists(csv_path):
                logging.debug(csv_path + " already exists - try next...")
                i += 1
                csv_path = "test/test_output_" + str(i) + ".csv"

            # write to csv
            dmi_df.to_csv(path_or_buf=csv_path)

        else:
            # create a table in the database (if it doesn't exist)
            create_dmi_table(db_engine)

            # create a temp file in the database
            create_temp_dmi_table(db_engine, uuid_str)

            # write df to temp table
            write_dmi_to_temp(dmi_df, db_engine, uuid_str)

            # merge temp table to main table (handling uspserts)
            merge_dmi_table(db_engine, uuid_str)

            # delete the temp table
            delete_temp_dmi_table(db_engine, uuid_str)

            # log success messsage
            logger.info('Dry matter intake file ' + filename + ' successfully written to database.')

    elif feedwatch_file_type == 'Ingredient Usage by Pen Type':

        # get the data from the file into a dataframe
        ingredient_df = import_ingredients_from_file(input_df, farm_id)

        if test:
            # find a csv name that doesn't exist
            i = 1
            csv_path = "test_output_" + str(i) + ".csv"
            while os.path.exists(csv_path):
                logging.debug(csv_path + " already exists - try next...")
                i += 1
                csv_path = "test_output_" + str(i) + ".csv"

            # write to csv
            ingredient_df.to_csv(path_or_buf=csv_path)

        else:
            # create a table in the database (if it doesn't exist)
            create_ingredients_table(db_engine)

            # create a temp file in the database
            create_temp_ingredients_table(db_engine, uuid_str)

            # write df to temp table
            write_ingredients_to_temp(ingredient_df, db_engine, uuid_str)

            # merge temp table to main table (handling uspserts)
            merge_ingredients_table(db_engine, uuid_str)

            # delete the temp table
            delete_temp_ingredients_table(db_engine, uuid_str)

            # log success
            logger.info('Ingredient file ' + filename + ' successfully written to database.')

    elif feedwatch_file_type == 'Pen Cost / Day Summary':

        # get the data from the file into a dataframe
        cost_df = import_cost_from_file(input_df, farm_id)

        if test:
            # find a csv name that doesn't exist
            i = 1
            csv_path = "test_output_" + str(i) + ".csv"
            while os.path.exists(csv_path):
                logging.debug(csv_path + " already exists - try next...")
                i += 1
                csv_path = "test_output_" + str(i) + ".csv"

            # write to csv
            cost_df.to_csv(path_or_buf=csv_path)

        else:
            # create a table in the database (if it doesn't exist)
            create_cost_table(db_engine)

            # create a temp file in the database
            create_temp_cost_table(db_engine, uuid_str)

            # write df to temp table
            write_cost_to_temp(cost_df, db_engine, uuid_str)

            # merge temp table to main table (handling uspserts)
            merge_cost_table(db_engine, uuid_str)

            # delete the temp table
            delete_temp_cost_table(db_engine, uuid_str)

            # log success
            logger.info('Cost file ' + filename + ' successfully written to database.')
    else:

        # error message and exit
        logging.error('File not recognized as dmi OR ingredient OR cost file!')
        exit(1)


##########################################################################################
# cost parsing
##########################################################################################

def import_cost_from_file(input_df, farm_id):
    # create write_df
    db_column_names = ["farm_id", "pen_name", "avg_pen_count", "expected_cost", "actual_cost", "report_date"]
    write_df = pd.DataFrame(columns=db_column_names)
    parsing_costs = False
    skiplines = {'Column', ''}

    for row in input_df.iterrows():

        first_col = row[1][0]
        logging.debug('check beginning of row: ' + str(first_col))

        # skip rows
        if str(first_col).strip() in skiplines or type(first_col) == float:
            logging.debug('skipping line starting with ' + str(first_col))

        # get the date range
        elif str(first_col) == 'ReportDate':
            if row[1][1] == 'between':
                start_date = (row[1][2].split(',')[0]).strip()
                end_date = (row[1][2].split(',')[1]).strip()
                logging.debug('Start date = ' + start_date + ', end date = ' + end_date)

        # start parsing when hit the 'Pen' header
        elif first_col.strip() == 'Pen':
            parsing_costs = True

        # finish when hitting feedwatch version message signaling
        elif str(first_col).startswith('FeedWatch'):
            return write_df

        else:
            # record the row for each date in the date range from the file
            record_date = datetime.datetime.strptime(start_date, "%m/%d/%Y").date()
            delta = datetime.timedelta(days=1)

            while record_date <= datetime.datetime.strptime(end_date, "%m/%d/%Y").date():
                # print('current record date: ' + record_date.strftime("%Y %m %d"))
                record_date += delta
                append_cost_row(farm_id, row, write_df, record_date)

            continue

    return write_df


def create_cost_table(db_engine):
    table_name = "feedwatch_cost_data"

    # check and delete if table already exists
    if not db_engine.dialect.has_table(db_engine, table_name):

        # create the new table
        create_cost_table_statement = text( \
            'CREATE TABLE ' + table_name + ' ( ' \
                                           'id                  serial primary key, ' \
                                           'farm_id             bigint, ' \
                                           'pen_name            varchar(255), ' \
                                           'report_date         date, ' \
                                           'expected_cost       float, ' \
                                           'actual_cost         float, ' \
                                           'avg_pen_count       bigint, ' \
                                           'UNIQUE (farm_id, pen_name, report_date));'
        )

        with db_engine.connect() as con:
            try:
                logger.info("Creating cost table...")
                logger.debug('create_cost_table statement = ' + str(create_cost_table_statement))
                con.execute(create_cost_table_statement)
            except Exception as e:
                logger.error("Error creating the feedwatch cost table in database!")
                logger.error(e.message)
                exit(1)


def create_temp_cost_table(db_engine, uuid_str):
    table_name = "temp_cost_" + uuid_str

    # check and delete if table already exists
    if db_engine.dialect.has_table(db_engine, table_name):
        logger.debug("Deleting old (pre-existing) temp_cost table...")
        delete_temp_cost_table(db_engine, uuid_str)

    # create the new table
    create_temp_table_statement = text( \
        'CREATE TABLE ' + table_name + ' ( ' \
                                       'id                  serial primary key, ' \
                                       'farm_id             bigint, ' \
                                       'pen_name            varchar(255), ' \
                                       'report_date         date, ' \
                                       'expected_cost       float, ' \
                                       'actual_cost         float, ' \
                                       'avg_pen_count       bigint, ' \
                                       'UNIQUE (farm_id, pen_name, report_date));'
    )

    with db_engine.connect() as con:
        try:
            logger.info("Creating temp_cost_table...")
            logger.debug('create_temp_cost_table statement = ' + str(create_temp_table_statement))
            con.execute(create_temp_table_statement)
        except Exception as e:
            logger.error("Error creating the temp_cost table from database!")
            logger.error(e.message)
            exit(1)


def append_cost_row(farm_id, row, write_df, record_date):
    # create an array with the data to be appended
    # ["farm_id", "pen_name", "avg_pen_count", "expected_cost", "actual_cost", "date"]
    new_row = [int(farm_id), row[1][0].encode('ascii'), int(row[1][1].encode('ascii')), get_dollar_value(row[1][2]),
               get_dollar_value(row[1][3]), record_date]
    logging.debug('appending new ingredient row: ' + str(new_row))
    # append the data to the dataframe in the last position
    write_df.loc[len(write_df)] = new_row


def write_cost_to_temp(cost_df, db_engine, uuid_str):
    table_name = "temp_cost_" + uuid_str

    # ["farm_id", "pen_name", "avg_pen_count", "expected_cost", "actual_cost", "report_date"]
    insert_statement = text('INSERT INTO ' + table_name + ' (farm_id, pen_name, report_date, avg_pen_count, ' \
                                                          'expected_cost, actual_cost) VALUES (:farm_id, :pen_name, :report_date, :avg_pen_count, :expected_cost, :actual_cost);')

    with db_engine.connect() as con:
        for row in cost_df.itertuples():

            data = ({"farm_id": row.farm_id, "pen_name": row.pen_name, "report_date": row.report_date, \
                     "avg_pen_count": row.avg_pen_count, "expected_cost": row.expected_cost,
                     "actual_cost": row.actual_cost})

            try:
                con.execute(insert_statement, data)
                logger.info("Inserting to temp_cost_table...")
                logger.debug('insert to temp_cost statement = ' + str(insert_statement))
            except Exception as e:
                logger.error("feedwatch_parser.py - write_cost_to_temp(): Error inserting to temp_cost table!")
                logger.error(e.message, e.args)
                exit(1)


def merge_cost_table(db_engine, uuid_str):
    # all this is mainly to get around the lack of 'ON CONFLICT' in postgres <9.5 - may also be more robust?

    table_name = "temp_cost_" + uuid_str

    merge_temp_table_statement = text(
        'BEGIN; ' \
        'LOCK TABLE feedwatch_cost_data IN EXCLUSIVE MODE;'

        'UPDATE feedwatch_cost_data ' \
        'SET (avg_pen_count, actual_cost, expected_cost) =  ' \
        '(' + table_name + '.avg_pen_count, ' + table_name + '.actual_cost, ' + table_name + '.expected_cost) ' \
                                                                                             'FROM ' + table_name + ' ' \
                                                                                                                    'WHERE ' + table_name + '.farm_id = feedwatch_cost_data.farm_id ' \
                                                                                                                                            'AND ' + table_name + '.pen_name = feedwatch_cost_data.pen_name ' \
                                                                                                                                                                  'AND ' + table_name + '.report_date = feedwatch_cost_data.report_date; ' \
                                                                                                                                                                                        'INSERT INTO feedwatch_cost_data (farm_id, avg_pen_count, actual_cost, expected_cost, ' \
                                                                                                                                                                                        'pen_name, report_date) ' \
                                                                                                                                                                                        '(SELECT ' + table_name + '.farm_id, ' + table_name + '.avg_pen_count, ' + table_name + '.actual_cost, ' + table_name + '.expected_cost, ' \
                                                                                                                                                                                                                                                                                                                '' + table_name + '.pen_name, ' + table_name + '.report_date ' \
                                                                                                                                                                                                                                                                                                                                                               'FROM ' + table_name + ' ' \
                                                                                                                                                                                                                                                                                                                                                                                      'LEFT OUTER JOIN feedwatch_cost_data ON (feedwatch_cost_data.farm_id = ' + table_name + '.farm_id AND feedwatch_cost_data.pen_name = ' + table_name + '.pen_name AND feedwatch_cost_data.report_date = ' + table_name + '.report_date) ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'WHERE feedwatch_cost_data.id IS NULL); ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'COMMIT;')

    with db_engine.connect() as con:
        try:
            con.execute(merge_temp_table_statement, {"uuid_str": uuid_str})
            logging.info("Merging temp_cost_table...")
            logging.debug('merge_cost_table statement = ' + str(merge_temp_table_statement))
        except Exception as e:
            logging.error("Error merging the temp_cost table to the feedwatch_cost table!")
            logger.error(e.message, e.args)
            exit(1)


def delete_temp_cost_table(db_engine, uuid_str):
    table_name = "temp_cost_" + uuid_str

    delete_temp_table_statement = text('DROP TABLE ' + table_name + ';')

    with db_engine.connect() as con:
        try:
            con.execute(delete_temp_table_statement)
            logging.info("Deleting temp_cost_table...")
            logging.debug('delete_temp_table_statement statement = ' + str(delete_temp_table_statement))
        except Exception as e:
            logging.error("Error deleting the temp_cost table from database!")
            logger.error(e.message, e.args)
            exit(1)


##########################################################################################
# DMI parsing
##########################################################################################

def import_dmi_from_file(input_df, farm_id):
    # create write_df
    db_column_names = ["farm_id", "company_name", "pen_name", "report_date", "avg_pen_count", "total_dropped",
                       "avg_dropped", "total_weighbacks", "avg_weighbacks", "total_dm", "avg_dm_animal"]
    write_df = pd.DataFrame(columns=db_column_names)
    last_was_totals = False
    parsing_pens = False
    pen_name = ""

    for row in input_df.iterrows():

        first_col = row[1][0]
        logging.debug('check beginning of row: ' + str(first_col))

        # skip total rows
        if str(first_col) == pen_name + " Totals:":
            last_was_totals = True

        # stop parsing when blank line after a totals is found
        elif not str(first_col).rstrip():
            if last_was_totals:
                return write_df

        # stop parsing when 'Grand totals' encountered - should be obsolete w/ blank line check (assuming that is consistent formatting from feedwatch)
        elif str(first_col).rstrip() == 'Grand Totals:':
            logging.debug('hit grand totals')
            return write_df

        # grab the company name
        elif first_col == 'CompanyName':
            company_name = row[1][2]

        # normal row parsing operation
        elif parsing_pens:
            logging.debug('parsing pens...')
            last_was_totals = False

            # check if first value (pen name) is empty
            if type(first_col) == str:
                if first_col:
                    logging.debug('changing pen_name from ' + pen_name + ' to ' + str(row[1][0]).rstrip())
                    pen_name = str(first_col).rstrip()
            append_dmi_row(farm_id, company_name, pen_name, row, write_df)

        # begin parsing when 'Pen Name' encountered
        elif str(first_col).rstrip() == '\r\nPen Name':
            logging.debug("first_col = \\r\\nPen Name")
            parsing_pens = True

        else:
            continue

    return write_df


def create_dmi_table(db_engine):
    table_name = "feedwatch_dmi_data"

    # check and delete if table already exists
    if not db_engine.dialect.has_table(db_engine, table_name):

        # create the new table
        create_dmi_table_statement = text( \
            'CREATE TABLE ' + table_name + ' ( ' \
                                           'id                  serial primary key, ' \
                                           'farm_id             bigint, ' \
                                           'company_name        varchar(255), ' \
                                           'pen_name            varchar(255), ' \
                                           'report_date         date, ' \
                                           'avg_pen_count       float, ' \
                                           'total_dropped       float, ' \
                                           'avg_dropped         float, ' \
                                           'total_weighbacks    float, ' \
                                           'avg_weighbacks      float, ' \
                                           'total_dm            float, ' \
                                           'avg_dm_animal       float, ' \
                                           'UNIQUE (farm_id, pen_name, report_date));'
        )

        with db_engine.connect() as con:
            try:
                logger.info("Creating dmi table...")
                logger.debug('create_dmi_table statement = ' + str(create_dmi_table_statement))
                con.execute(create_dmi_table_statement)
            except Exception as e:
                logger.error("Error creating the feedwatch_dmi_table in database!")
                logger.error(e.message)
                exit(1)


def create_temp_dmi_table(db_engine, uuid_str):
    table_name = "temp_dmi_" + uuid_str

    # check and delete if table already exists
    if db_engine.dialect.has_table(db_engine, table_name):
        logger.debug("Deleting old (pre-existing) temp_dmi table...")
        delete_temp_dmi_table(db_engine, uuid_str)

    # create the new table
    create_temp_table_statement = text( \
        'CREATE TABLE ' + table_name + ' ( ' \
                                       'id                  serial primary key, ' \
                                       'farm_id             bigint, ' \
                                       'company_name        varchar(255), ' \
                                       'pen_name            varchar(255), ' \
                                       'report_date         date, ' \
                                       'avg_pen_count       float, ' \
                                       'total_dropped       float, ' \
                                       'avg_dropped         float, ' \
                                       'total_weighbacks    float, ' \
                                       'avg_weighbacks      float, ' \
                                       'total_dm            float, ' \
                                       'avg_dm_animal       float, ' \
                                       'UNIQUE (farm_id, pen_name, report_date));'
    )

    with db_engine.connect() as con:
        try:
            logger.info("Creating temp_dmi_table...")
            logger.debug('create_temp_dmi_table statement = ' + str(create_temp_table_statement))
            con.execute(create_temp_table_statement)
        except Exception as e:
            logger.error("Error creating the temp_dmi table from database!")
            logger.error(e.message)
            exit(1)


def append_dmi_row(farm_id, company_name, pen_name, row, write_df):
    # create an array with the data to be appended
    # ["farm_id", "company_name", "pen_name", "report_date", "avg_pen_count", "total_dropped",
    #    "avg_dropped", "total_weighbacks", "avg_weighback", "total_dm", "avg_dm_animal"]
    try:
        new_row = [int(farm_id), company_name, pen_name, row[1][1], row[1][2], get_a_float(row[1][3]),
                   get_a_float(row[1][4]), get_a_float(row[1][5]), get_a_float(row[1][6]), get_a_float(row[1][7]),
                   get_a_float(row[1][8])]
    except Exception as e:
        logging.error("feedwatch_parser.append_dmi_row() - exception raised!")
        logger.error(e.message, e.args)
    logging.debug('appending new dmi row: ' + str(new_row))
    # append the data to the dataframe in the last position
    write_df.loc[len(write_df)] = new_row


def write_dmi_to_temp(dmi_df, db_engine, uuid_str):
    table_name = "temp_dmi_" + uuid_str

    insert_statement = text(
        'INSERT INTO ' + table_name + ' (farm_id, company_name, pen_name, report_date, avg_pen_count, ' \
                                      'total_dropped, avg_dropped, total_weighbacks, avg_weighbacks, total_dm, avg_dm_animal) VALUES (:farm_id, ' \
                                      ':company_name, :pen_name, :report_date, :avg_pen_count, :total_dropped, :avg_dropped, :total_weighbacks, ' \
                                      ':avg_weighbacks, :total_dm, :avg_dm_animal);')

    with db_engine.connect() as con:
        for row in dmi_df.itertuples():

            data = ({"farm_id": row.farm_id, "company_name": row.company_name, "pen_name": row.pen_name,
                     "report_date": row.report_date, \
                     "avg_pen_count": row.avg_pen_count, "total_dropped": row.total_dropped,
                     "avg_dropped": row.avg_dropped, \
                     "total_weighbacks": row.total_weighbacks, "avg_weighbacks": row.avg_weighbacks,
                     "total_dm": row.total_dm, \
                     "avg_dm_animal": row.avg_dm_animal})

            try:
                con.execute(insert_statement, data)
                logger.info("Inserting to temp_dmi_table...")
                logger.debug('insert to temp_dmi statement = ' + str(insert_statement))
            except Exception as e:
                logger.error("feedwatch_parser.py - write_dmi_to_temp(): Error inserting to temp_dmi table!")
                logger.error(e.message, e.args)
                exit(1)


def merge_dmi_table(db_engine, uuid_str):
    # all this is mainly to get around the lack of 'ON CONFLICT' in postgres <9.5 - may also be more robust?

    table_name = "temp_dmi_" + uuid_str

    merge_temp_table_statement = text(
        'BEGIN; ' \
        'LOCK TABLE feedwatch_dmi_data IN EXCLUSIVE MODE;'

        'UPDATE feedwatch_dmi_data ' \
        'SET (avg_pen_count, total_dropped, avg_dropped, total_weighbacks, avg_weighbacks, total_dm, avg_dm_animal) =  ' \
        '(' + table_name + '.avg_pen_count, ' + table_name + '.total_dropped, ' + table_name + '.avg_dropped, ' + table_name + '.total_weighbacks, ' + table_name + '.avg_weighbacks, ' + table_name + '.total_dm, ' + table_name + '.avg_dm_animal) ' \
                                                                                                                                                                                                                                    'FROM ' + table_name + ' ' \
                                                                                                                                                                                                                                                           'WHERE ' + table_name + '.farm_id = feedwatch_dmi_data.farm_id ' \
                                                                                                                                                                                                                                                                                   'AND ' + table_name + '.pen_name = feedwatch_dmi_data.pen_name ' \
                                                                                                                                                                                                                                                                                                         'AND ' + table_name + '.report_date = feedwatch_dmi_data.report_date; ' \
                                                                                                                                                                                                                                                                                                                               'INSERT INTO feedwatch_dmi_data (farm_id, company_name, pen_name, report_date, avg_pen_count, ' \
                                                                                                                                                                                                                                                                                                                               'total_dropped, avg_dropped, total_weighbacks, avg_weighbacks, total_dm, avg_dm_animal) ' \
                                                                                                                                                                                                                                                                                                                               '(SELECT ' + table_name + '.farm_id, ' + table_name + '.company_name, ' + table_name + '.pen_name, ' + table_name + '.report_date, ' + table_name + '.avg_pen_count, ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   '' + table_name + '.total_dropped, ' + table_name + '.avg_dropped, ' + table_name + '.total_weighbacks, ' + table_name + '.avg_weighbacks, ' + table_name + '.total_dm, ' + table_name + '.avg_dm_animal ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            'FROM ' + table_name + ' ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   'LEFT OUTER JOIN feedwatch_dmi_data ON (feedwatch_dmi_data.farm_id = ' + table_name + '.farm_id AND feedwatch_dmi_data.pen_name = ' + table_name + '.pen_name AND feedwatch_dmi_data.report_date = ' + table_name + '.report_date) ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       'WHERE feedwatch_dmi_data.id IS NULL); ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       'COMMIT;')

    with db_engine.connect() as con:
        try:
            con.execute(merge_temp_table_statement, {"uuid_str": uuid_str})
            logging.info("Merging temp_dmi_table...")
            logging.debug('merge_dmi_table statement = ' + str(merge_temp_table_statement))
        except Exception as e:
            logging.error("Error merging the temp_dmi table to the feedwatch_dmi_data table!")
            logger.error(e.message, e.args)
            exit(1)


def delete_temp_dmi_table(db_engine, uuid_str):
    table_name = "temp_dmi_" + uuid_str

    delete_temp_table_statement = text('DROP TABLE ' + table_name + ';')

    with db_engine.connect() as con:
        try:
            con.execute(delete_temp_table_statement)
            logging.info("Deleting temp_dmi_table...")
            logging.debug('delete_temp_table_statement statement = ' + str(delete_temp_table_statement))
        except Exception as e:
            logging.error("Error deleting the temp_dmi table from database!")
            logger.error(e.message, e.args)
            exit(1)


##########################################################################################
# ingredient parsing
##########################################################################################

def import_ingredients_from_file(input_df, farm_id):
    # create write_df
    db_column_names = ["farm_id", "company_name", "pen_type", "ingredient_name", "as_fed_quantity", "as_fed_tons",
                       "report_date"]
    write_df = pd.DataFrame(columns=db_column_names)
    last_was_totals = False
    parsing_ingredients = False
    pen_type = ''
    skiplines = {'Column', 'Ingredient', ''}

    for row in input_df.iterrows():

        first_col = row[1][0]
        logging.debug('check beginning of row: ' + str(first_col))

        # skip rows
        if str(first_col).strip() in skiplines or type(first_col) == float:
            logging.debug('skipping line starting with ' + str(first_col))

        # grab the company name
        elif first_col == 'CompanyName':
            company_name = row[1][2]

        # get the date range
        elif str(first_col) == 'ReportDate':
            if row[1][1] == 'between':
                start_date = (row[1][2].split(',')[0]).strip()
                end_date = (row[1][2].split(',')[1]).strip()
                logging.debug('Start date = ' + start_date + ', end date = ' + end_date)

        # encounter and store a pen type
        elif pen_type == '':
            logging.debug('pen_type registered as ''; pen_type = ' + pen_type + '; setting pen_type to ' + first_col)
            pen_type = first_col

        elif first_col == pen_type:
            pen_type = ''

        elif str(first_col).startswith('FeedWatch'):
            return write_df

        else:
            # record the row for each date in the date range from the file
            record_date = datetime.datetime.strptime(start_date, "%m/%d/%Y").date()
            delta = datetime.timedelta(days=1)

            while record_date <= datetime.datetime.strptime(end_date, "%m/%d/%Y").date():
                # print('current record date: ' + record_date.strftime("%Y %m %d"))
                record_date += delta
                append_ingredient_row(farm_id, company_name, pen_type, row, write_df, record_date)

            continue

    return write_df


def create_ingredients_table(db_engine):
    table_name = "feedwatch_ingredients_data"

    # check and delete if table already exists
    if not db_engine.dialect.has_table(db_engine, table_name):

        # create the new table
        create_ingredients_table_statement = text( \
            'CREATE TABLE ' + table_name + ' ( ' \
                                           'id                  serial primary key,' \
                                           'farm_id             bigint,' \
                                           'company_name        varchar(255),' \
                                           'pen_type            varchar(255),' \
                                           'ingredient_name     varchar(255),' \
                                           'as_fed_quantity     float,' \
                                           'as_fed_tons         float,' \
                                           'report_date         date,' \
                                           'UNIQUE (farm_id, pen_type, ingredient_name, report_date));' \
            )

        with db_engine.connect() as con:
            try:
                logger.info("Creating ingredients table...")
                logger.debug('create_ingredients_table statement = ' + str(create_ingredients_table_statement))
                con.execute(create_ingredients_table_statement)
            except Exception as e:
                logger.error("Error creating the feedwatch_ingredients_table in database!")
                logger.error(e.message)
                exit(1)


def create_temp_ingredients_table(db_engine, uuid_str):
    table_name = "temp_ingredient_" + uuid_str

    # check and delete if table already exists
    if db_engine.dialect.has_table(db_engine, table_name):
        logger.debug("Deleting old (pre-existing) temp_ingredients table...")
        delete_temp_dmi_table(db_engine, uuid_str)

    # create the new table
    create_temp_table_statement = text( \
        'CREATE TABLE ' + table_name + ' ( ' \
                                       'id                  serial primary key,' \
                                       'farm_id             bigint,' \
                                       'company_name        varchar(255),' \
                                       'pen_type            varchar(255),' \
                                       'ingredient_name     varchar(255),' \
                                       'as_fed_quantity     float,' \
                                       'as_fed_tons         float,' \
                                       'report_date         date,' \
                                       'UNIQUE (farm_id, pen_type, ingredient_name, report_date));' \
        )

    with db_engine.connect() as con:
        try:
            logger.info("Creating temp_ingredients_table...")
            logger.debug('create_temp_ingredients_table statement = ' + str(create_temp_table_statement))
            con.execute(create_temp_table_statement)
        except Exception as e:
            logger.error("Error creating the temp_ingredients table from database!")
            logger.error(e.message)
            exit(1)


def append_ingredient_row(farm_id, company_name, pen_type, row, write_df, record_date):
    # create an array with the data to be appended
    # ["farm_id", "company_name", "pen_type", "ingredient_name", "as_fed_quantity", "as_fed_tons", "date"]
    new_row = [int(farm_id), company_name, pen_type, row[1][0], get_a_float(row[1][2]), get_a_float(row[1][3]),
               record_date]
    logging.debug('appending new ingredient row: ' + str(new_row))
    # append the data to the dataframe in the last position
    write_df.loc[len(write_df)] = new_row


def write_ingredients_to_temp(dmi_df, db_engine, uuid_str):
    table_name = "temp_ingredient_" + uuid_str

    insert_statement = text('INSERT INTO ' + table_name + '(farm_id, company_name, pen_type, ingredient_name, ' \
                                                          'as_fed_quantity, as_fed_tons, report_date) VALUES (:farm_id, :company_name, :pen_type, :ingredient_name, ' \
                                                          ':as_fed_quantity, :as_fed_tons, :report_date);')

    with db_engine.connect() as con:
        for row in dmi_df.itertuples():

            data = ({"farm_id": row.farm_id, "company_name": row.company_name, "pen_type": row.pen_type,
                     "ingredient_name": row.ingredient_name, \
                     "as_fed_quantity": row.as_fed_quantity, "as_fed_tons": row.as_fed_tons,
                     "report_date": row.report_date})

            try:
                con.execute(insert_statement, data)
                logger.info("Inserting to temp_ingredients_table...")
                # logger.debug('insert to temp_ingredients statement = ' + str(insert_statement))
            except Exception as e:
                logger.error("feedwatch_parser.py - write_ingredients_to_temp(): Error inserting to temp_dmi table!")
                logger.error(e.message, e.args)
                exit(1)


def delete_temp_ingredients_table(db_engine, uuid_str):
    table_name = "temp_ingredient_" + uuid_str

    delete_temp_table_statement = text('DROP TABLE ' + table_name + ';')

    with db_engine.connect() as con:
        try:
            con.execute(delete_temp_table_statement)
            logging.info("Deleting temp_ingredients_table...")
            logging.debug('delete_temp_table_statement statement = ' + str(delete_temp_table_statement))
        except Exception as e:
            logging.error("Error deleting the temp_ingredients table from database!")
            logger.error(e.message, e.args)
            exit(1)


def merge_ingredients_table(db_engine, uuid_str):
    # all this is mainly to get around the lack of 'ON CONFLICT' in postgres <9.5 - may also be more robust?

    table_name = "temp_ingredient_" + uuid_str

    merge_temp_table_statement = text(
        'BEGIN; ' \
        'LOCK TABLE feedwatch_ingredients_data IN EXCLUSIVE MODE;'

        'UPDATE feedwatch_ingredients_data ' \
        'SET (as_fed_quantity, as_fed_tons) =  ' \
        '(' + table_name + '.as_fed_quantity, ' + table_name + '.as_fed_tons) ' \
                                                               'FROM ' + table_name + ' ' \
                                                                                      'WHERE ' + table_name + '.farm_id = feedwatch_ingredients_data.farm_id ' \
                                                                                                              'AND ' + table_name + '.pen_type = feedwatch_ingredients_data.pen_type ' \
                                                                                                                                    'AND ' + table_name + '.ingredient_name = feedwatch_ingredients_data.ingredient_name ' \
                                                                                                                                                          'AND ' + table_name + '.report_date = feedwatch_ingredients_data.report_date; ' \
                                                                                                                                                                                'INSERT INTO feedwatch_ingredients_data (farm_id, company_name, pen_type, ingredient_name, ' \
                                                                                                                                                                                'as_fed_quantity, as_fed_tons, report_date) ' \
                                                                                                                                                                                '(SELECT ' + table_name + '.farm_id, ' + table_name + '.company_name, ' + table_name + '.pen_type, ' + table_name + '.ingredient_name, ' \
                                                                                                                                                                                                                                                                                                    '' + table_name + '.as_fed_quantity, ' + table_name + '.as_fed_tons, ' + table_name + '.report_date ' \
                                                                                                                                                                                                                                                                                                                                                                                          'FROM ' + table_name + ' ' \
                                                                                                                                                                                                                                                                                                                                                                                                                 'LEFT OUTER JOIN feedwatch_ingredients_data ' \
                                                                                                                                                                                                                                                                                                                                                                                                                 'ON (feedwatch_ingredients_data.farm_id = ' + table_name + '.farm_id ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                            'AND feedwatch_ingredients_data.pen_type = ' + table_name + '.pen_type ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        'AND feedwatch_ingredients_data.ingredient_name = ' + table_name + '.ingredient_name ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           'AND feedwatch_ingredients_data.report_date = ' + table_name + '.report_date) ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'WHERE feedwatch_ingredients_data.id IS NULL); ' \
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'COMMIT;')

    with db_engine.connect() as con:
        try:
            con.execute(merge_temp_table_statement, {"uuid_str": uuid_str})
            logging.info("Merging temp_ingredients_table...")
            # logging.debug('merge_dmi_table statement = ' + str(merge_temp_table_statement))
        except Exception as e:
            logging.error("Error merging the temp_ingredients table to the feedwatch_ingredients_data table!")
            logger.error(e.message, e.args)
            exit(1)


# depricated to use temp table and merge for backwards compatability with postgres < 9.5
def write_dmi_to_db(dmi_df, db_engine):
    upsert_statement = text(
        'INSERT INTO feedwatch_dmi_data(farm_id, company_name, pen_name, report_date, avg_pen_count, ' \
        'total_dropped, avg_dropped, total_weighbacks, avg_weighbacks, total_dm, avg_dm_animal) VALUES (:farm_id, ' \
        ':company_name, :pen_name, :report_date, :avg_pen_count, :total_dropped, :avg_dropped, :total_weighbacks, ' \
        ':avg_weighbacks, :total_dm, :avg_dm_animal) ' \
        'ON CONFLICT (farm_id, pen_name, report_date) ' \
        'DO UPDATE SET (avg_pen_count, total_dropped, avg_dropped, total_weighbacks, avg_weighbacks, total_dm, ' \
        'avg_dm_animal) = (:avg_pen_count, :total_dropped, :avg_dropped, :total_weighbacks, :avg_weighbacks, ' \
        ':total_dm, :avg_dm_animal);')

    with db_engine.connect() as con:
        for row in dmi_df.itertuples():
            data = ({"farm_id": row.farm_id, "company_name": row.company_name, "pen_name": row.pen_name,
                     "report_date": row.report_date, \
                     "avg_pen_count": row.avg_pen_count, "total_dropped": row.total_dropped,
                     "avg_dropped": row.avg_dropped, \
                     "total_weighbacks": row.total_weighbacks, "avg_weighbacks": row.avg_weighbacks,
                     "total_dm": row.total_dm, \
                     "avg_dm_animal": row.avg_dm_animal})

            con.execute(upsert_statement, data)

        # depricated to use temp table and merge for backwards compatability with postgres < 9.5


def write_ingredients_to_db(ingredient_df, db_engine):
    # "farm_id", "company_name", "pen_type", "ingredient_name", "as_fed_quantity", "as_fed_tons", "report_date"

    upsert_statement = text('INSERT INTO feedwatch_ingredients_data(farm_id, company_name, pen_type, ingredient_name, ' \
                            'as_fed_quantity, as_fed_tons, report_date) VALUES (:farm_id, :company_name, :pen_type, :ingredient_name, ' \
                            ':as_fed_quantity, :as_fed_tons, :report_date) ' \
                            'ON CONFLICT (farm_id, pen_type, ingredient_name, report_date) ' \
                            'DO UPDATE SET (as_fed_quantity, as_fed_tons) = (:as_fed_quantity, :as_fed_tons);')

    with db_engine.connect() as con:
        for row in ingredient_df.itertuples():
            data = ({"farm_id": row.farm_id, "company_name": row.company_name, "pen_type": row.pen_type,
                     "ingredient_name": row.ingredient_name, \
                     "as_fed_quantity": row.as_fed_quantity, "as_fed_tons": row.as_fed_tons,
                     "report_date": row.report_date})

            con.execute(upsert_statement, data)


def get_a_float(input):
    if type(input) == float:
        return input
    elif type(input) == str:
        return float(input.replace(',', ''))


def get_dollar_value(str_value):
    if (str_value.lstrip() == '-'):
        return None
    else:
        value = float(str_value[1:].encode('ascii'))
        return value