#!/usr/bin/env python
""" Parsing and importing dhi data files from agsource
    into a database or csv """

import pandas as pd
import math
import datetime
import logging
import uuid
import os
import pdb
from sqlalchemy.sql import text

__author__ = "Steven Wangen"
__version__ = "1.0.1"
__email__ = "srwangen@wisc.edu"
__status__ = "Development"

logger = logging.getLogger(__name__)


def parse_components(test, farm_id, filename, db_engine):
    # set file encoding (dunno why no utf-8 - legacy?)
    file_encoding = "ISO-8859-1"

    # dump raw input into df
    input_df = pd.read_excel(filename)

    # create a uuid for a unique temp table
    uuid_str = uuid.uuid4().hex

    if headers_checkout(input_df):
        logging.info('headers check out...')

        if test:
            # find a csv name that doesn't exist
            i = 1
            csv_path = "test_component_output_" + str(i) + ".csv"
            while os.path.exists(csv_path):
                logging.debug(csv_path + " already exists - try next...")
                i += 1
                csv_path = "test_component_output_" + str(i) + ".csv"

            # write to csv
            input_df.to_csv(path_or_buf=csv_path)
            return 'component', True

        else:
            # transform input_df to a reformatted write_df
            write_df = import_components_from_file(input_df, farm_id)

            if (type(write_df) == bool) and (write_df == False):
                return 'component', False

            # generate a temporary table name
            temp_table_name = "ag_source.temp_components_" + uuid_str

            # create a temp table in the database
            create_temp_components_table(db_engine, temp_table_name)

            # write df to temp table
            write_components_to_temp(write_df, db_engine, temp_table_name)

            # merge temp table to main table (handling uspserts)
            merge_components_table(db_engine, temp_table_name)

            # delete the temp table
            delete_temp_components_table(db_engine, temp_table_name)

            # log success messsage
            logger.info('Components intake file ' + filename + ' successfully written to database.')
            return 'component', True
    else:
        logging.error("agsource_parser.parse_file - the headers don't check out for " + filename + "!!!")
        return 'component', False


def parse_spectrum(test, farm_id, filename, db_engine):
    # set file encoding (dunno why no utf-8 - legacy?)
    file_encoding = "ISO-8859-1"

    # dump raw input into df
    input_df = pd.read_excel(filename)

    # create a uuid for a unique temp table
    uuid_str = uuid.uuid4().hex

    if test:
        # find a csv name that doesn't exist
        i = 1
        csv_path = "test_spectral_output_" + str(i) + ".csv"
        while os.path.exists(csv_path):
            logging.debug(csv_path + " already exists - try next...")
            i += 1
            csv_path = "test_spectral_output_" + str(i) + ".csv"

        # write to csv
        input_df.to_csv(path_or_buf=csv_path)
        return 'spectrum', True

    else:
        # generate a temporary table name
        temp_table_name = "ag_source.temp_spectrum_" + uuid_str

        # transform input_df to a reformatted write_df
        write_df = import_spectrum_from_file(input_df, farm_id)

        if (type(write_df) == bool) and (write_df == False):
            return 'spectrum', False

        # create a temp file in the database
        create_temp_spectrum_table(db_engine, temp_table_name)

        # write df to temp table
        write_spectrum_to_temp(write_df, db_engine, temp_table_name)

        # merge temp table to main table (handling uspserts)
        merge_spectrum_table(db_engine, temp_table_name)

        # delete the temp table
        delete_temp_spectrum_table(db_engine, temp_table_name)

        # log success
        logger.info('Spectral data file ' + filename + ' successfully written to database.')
        return 'spectrum', True


##########################################################################################
# Conponent parsing
##########################################################################################

def import_components_from_file(input_df, farm_id):
    # get the date from the first row (column names - wtf)
    sample_date = input_df.columns[2]
    if type(sample_date) != datetime.datetime:
        logging.error(
            "agsource_parser.import_components_from_file(): sample date not registering as datetime.datetime!")
        return False

    # create write_df to hold data from file before writing to database
    db_column_names = ['farm_id', 'sample_date', 'cntl', 'bname', 'sample_id', 'dim', 'milk', 'fat', 'true_prot',
                       'lactose', 'snf',
                       'cells', 'urea', 'mono_unsaturated_fa', 'poly_unsaturated_fa', 'saturated_fa',
                       'tot_unsaturated_fat', 'scfa',
                       'mcfa', 'lcfa', 'trans', 'aceton', 'bhb', 'C14_0', 'C16_0', 'C18_0', 'C18_1', 'de_novo_fa',
                       'mixed_fa', 'preformed_fa']

    write_df = pd.DataFrame(columns=db_column_names)

    # iterate over rows in file
    for row in input_df.iterrows():

        # skip header rows
        if type((row[1].to_list())[0]) == int:
            da_row = row[1].to_list()
            append_component_row(farm_id, sample_date, da_row, write_df)

            # remove the 'remark' column which is present in SOME csv's
            # if len(da_row) == 29:
            #     del da_row[11]
            # if len(da_row) == 28:
            #     
            # else:
            #    logging.error('There are too many columns present in the data file!')
            #    sys.exit(1)

    return write_df


def create_temp_components_table(db_engine, table_name):
    # check and delete if table already exists
    if db_engine.dialect.has_table(db_engine, table_name):
        logger.debug("Deleting old (pre-existing) temp_components table...")
        delete_temp_components_table(db_engine, table_name)

    # create the new table
    create_temp_table_statement = text( \
        'CREATE TABLE ' + table_name + ' ( ' \
                                       'id                  serial primary key,' \
                                       'farm_id             bigint,' \
                                       'sample_date         date,' \
                                       'cntl                int,' \
                                       'bname               int,' \
                                       'sample_id           int,' \
                                       'dim                 int,' \
                                       'milk                int,' \
                                       'fat                 float,' \
                                       'true_prot           float,' \
                                       'lactose             float,' \
                                       'snf                 float,' \
                                       'cells               int,' \
                                       'urea                float,' \
                                       'mono_unsaturated_fa float,' \
                                       'poly_unsaturated_fa float,' \
                                       'saturated_fa        float,' \
                                       'tot_unsaturated_fat float,' \
                                       'scfa                float,' \
                                       'mcfa                float,' \
                                       'lcfa                float,' \
                                       'trans               float,' \
                                       'aceton              float,' \
                                       'bhb                 float,' \
                                       'C14_0               float,' \
                                       'C16_0               float,' \
                                       'C18_0               float,' \
                                       'C18_1               float,' \
                                       'de_novo_fa          float,' \
                                       'mixed_fa            float,' \
                                       'preformed_fa        float,' \
                                       'sample_order        int,' \
                                       'UNIQUE(farm_id, sample_id, sample_date));' \
        )

    with db_engine.connect() as con:
        try:
            logger.info("Creating temp_component_table...")
            logger.debug('create_temp_component_table statement = ' + str(create_temp_table_statement))
            con.execute(create_temp_table_statement)
        except Exception as e:
            logger.error("Error creating the temp_component table from database!")
            logger.error(e.args)
            exit(1)


def append_component_row(farm_id, sample_date, row, write_df):
    # create an array with the data to be appended
    # ['id', 'farm_id', 'sample_date', 'cow_id', 'sample_id', 'fat', 'true_prot', 'lactose', 'snf', 'cells', 'urea', 
    #  'mono_unsaturated_fa', 'poly_unsaturated_fa', 'saturated_fa', 'scfa',
    # 'mcfa', 'lcfa', 'trans', 'fa', 'aceton', 'bhb',
    #  'C14_0', 'C16_0', 'C18_0', 'C18_1']

    # new 
    # [id, farm_id, sample_date, cntl, bname, sample_id, dim, milk, fat, true_prot, lactose, snf, cells, 
    # urea, mono_unsaturated_fa, poly_unsaturated_fa, saturated_fa, tot_unsaturated_fat, scfa, mcfa, lcfa,
    # trans, aceton, bhb, C14_0, C16_0, C18_0, C18_1, de_novo_fa, mixed_fa, preformed_fa]
    try:
        new_row = [int(farm_id),
                   sample_date,
                   get_an_int(row[0]),
                   get_an_int(row[1]),
                   get_an_int(row[2]),
                   get_an_int(row[3]),
                   get_an_int(row[4]),
                   get_a_float(row[5]),
                   get_a_float(row[6]),
                   get_a_float(row[7]),
                   get_a_float(row[8]),
                   get_an_int(row[9]),
                   get_a_float(row[10]),
                   get_a_float(row[11]),
                   get_a_float(row[12]),
                   get_a_float(row[13]),
                   get_a_float(row[14]),
                   get_a_float(row[15]),
                   get_a_float(row[16]),
                   get_a_float(row[17]),
                   get_a_float(row[18]),
                   get_a_float(row[19]),
                   get_a_float(row[20]),
                   get_a_float(row[21]),
                   get_a_float(row[22]),
                   get_a_float(row[23]),
                   get_a_float(row[24]),
                   get_a_float(row[25]),
                   get_a_float(row[26]),
                   get_a_float(row[27])]

    except Exception as e:
        logging.error("agsource_parser.append_component_row() - exception raised!")
        logger.error(e, e.args)
    logging.debug('appending new component row: ' + str(new_row))

    # append the data to the dataframe in the last position
    write_df.loc[len(write_df)] = new_row


def write_components_to_temp(write_df, db_engine, table_name):
    insert_statement = text('INSERT INTO ' + table_name + ' (farm_id, sample_date, cntl, bname, sample_id, dim, milk, fat, true_prot, \
        lactose, snf, cells, urea, mono_unsaturated_fa, poly_unsaturated_fa, saturated_fa, tot_unsaturated_fat, scfa, mcfa, \
        lcfa, trans, aceton, bhb, C14_0, C16_0, C18_0, C18_1, de_novo_fa, mixed_fa, preformed_fa, sample_order) VALUES (:farm_id, \
        :sample_date, :cntl, :bname, :sample_id, :dim, :milk, :fat, :true_prot, :lactose, :snf, :cells, :urea, \
        :mono_unsaturated_fa, :poly_unsaturated_fa, :saturated_fa, :tot_unsaturated_fat, :scfa, :mcfa, :lcfa, :trans, \
        :aceton, :bhb, :C14_0, :C16_0, :C18_0, :C18_1, :de_novo_fa, :mixed_fa, :preformed_fa, :sample_order);')

    with db_engine.connect() as con:
        sample_order = 0
        for row in write_df.itertuples():

            sample_order += 1

            data = ({"farm_id": row.farm_id, "sample_date": row.sample_date, "cntl": row.cntl, "bname": row.bname, \
                     "sample_id": row.sample_id, "dim": row.dim, "milk": row.milk, "fat": row.fat,
                     "true_prot": row.true_prot, \
                     "lactose": row.lactose, "snf": row.snf, "cells": row.cells, "urea": row.urea,
                     "mono_unsaturated_fa": row.mono_unsaturated_fa, \
                     "poly_unsaturated_fa": row.poly_unsaturated_fa, "saturated_fa": row.saturated_fa, \
                     "tot_unsaturated_fat": row.tot_unsaturated_fat, "scfa": row.scfa, "mcfa": row.mcfa,
                     "lcfa": row.lcfa, \
                     "trans": row.trans, "aceton": row.aceton, "bhb": row.bhb, "C14_0": row.C14_0, "C16_0": row.C16_0, \
                     "C18_0": row.C18_0, "C18_1": row.C18_1, "de_novo_fa": row.de_novo_fa, "mixed_fa": row.mixed_fa, \
                     "preformed_fa": row.preformed_fa, "sample_order": sample_order})

            try:
                con.execute(insert_statement, data)
                logger.debug('insert to temp_component statement = ' + str(insert_statement))
            except Exception as e:
                logger.error("agsource_parser.py - write_component_to_temp(): Error inserting to temp_component table!")
                logger.error(e.args)
                exit(1)


def merge_components_table(db_engine, temp_table_name):
    # all this is mainly to get around the lack of 'ON CONFLICT' in postgres <9.5 - may also be more robust?

    table_name = 'ag_source.components'

    merge_temp_table_statement = text(
        'BEGIN; \
        LOCK TABLE {to_table} IN EXCLUSIVE MODE; \
        UPDATE {to_table} \
        SET (farm_id, sample_date, cntl, bname, sample_id, dim, milk, fat, true_prot, lactose, snf, cells, urea, mono_unsaturated_fa, \
        poly_unsaturated_fa, saturated_fa, tot_unsaturated_fat, scfa, mcfa, lcfa, trans, aceton, bhb, C14_0, C16_0, C18_0, C18_1, \
        de_novo_fa, mixed_fa, preformed_fa, sample_order) =  \
        ( {temp_table}.farm_id, {temp_table}.sample_date, {temp_table}.cntl, {temp_table}.bname, {temp_table}.sample_id, \
        {temp_table}.dim, {temp_table}.milk, {temp_table}.fat, {temp_table}.true_prot, {temp_table}.lactose, {temp_table}.snf, \
        {temp_table}.cells, {temp_table}.urea, {temp_table}.mono_unsaturated_fa, {temp_table}.poly_unsaturated_fa, \
        {temp_table}.saturated_fa, {temp_table}.tot_unsaturated_fat, {temp_table}.scfa, {temp_table}.mcfa, {temp_table}.lcfa, \
        {temp_table}.trans, {temp_table}.aceton, {temp_table}.bhb, {temp_table}.C14_0, {temp_table}.C16_0, {temp_table}.C18_0, \
        {temp_table}.C18_1, {temp_table}.de_novo_fa, {temp_table}.mixed_fa, {temp_table}.preformed_fa, {temp_table}.sample_order) \
        FROM  {temp_table} \
        WHERE  {to_table}.farm_id = {temp_table}.farm_id \
        AND  {to_table}.sample_date = {temp_table}.sample_date \
        AND  {to_table}.sample_id = {temp_table}.sample_id \
        AND  {to_table}.cntl = {temp_table}.cntl;\
        INSERT INTO {to_table} (farm_id, sample_date, cntl, bname, sample_id, dim, milk, fat, true_prot, lactose, snf, cells, \
        urea, mono_unsaturated_fa, poly_unsaturated_fa, saturated_fa, tot_unsaturated_fat, scfa, mcfa, lcfa, trans, aceton, \
        bhb, C14_0, C16_0, C18_0, C18_1, de_novo_fa, mixed_fa, preformed_fa, sample_order) \
        SELECT {temp_table}.farm_id, {temp_table}.sample_date, {temp_table}.cntl, {temp_table}.bname, {temp_table}.sample_id, \
        {temp_table}.dim, {temp_table}.milk, {temp_table}.fat, {temp_table}.true_prot, {temp_table}.lactose, {temp_table}.snf, \
        {temp_table}.cells, {temp_table}.urea, {temp_table}.mono_unsaturated_fa, {temp_table}.poly_unsaturated_fa, \
        {temp_table}.saturated_fa, {temp_table}.tot_unsaturated_fat, {temp_table}.scfa, {temp_table}.mcfa, {temp_table}.lcfa, \
        {temp_table}.trans, {temp_table}.aceton, {temp_table}.bhb, {temp_table}.C14_0, {temp_table}.C16_0, {temp_table}.C18_0, \
        {temp_table}.C18_1, {temp_table}.de_novo_fa, {temp_table}.mixed_fa, {temp_table}.preformed_fa, {temp_table}.sample_order  \
        FROM  {temp_table} \
        LEFT OUTER JOIN {to_table} ON \
        ({to_table}.farm_id = {temp_table}.farm_id \
        AND {to_table}.sample_date = {temp_table}.sample_date \
        AND {to_table}.sample_id = {temp_table}.sample_id \
        AND {to_table}.cntl = {temp_table}.cntl) \
        WHERE {to_table}.id IS NULL; \
        COMMIT;'.format(to_table=table_name, temp_table=temp_table_name))

    with db_engine.connect() as con:
        try:
            con.execute(merge_temp_table_statement)
            logging.info("Merging temp_component_table...")
            logging.debug('merge_component_table statement = ' + str(merge_temp_table_statement))
        except Exception as e:
            logging.error("Error merging the temp_component table to the ag_source.component table!")
            logger.error(e.args)
            exit(1)


def delete_temp_components_table(db_engine, table_name):
    delete_temp_table_statement = text('DROP TABLE ' + table_name + ';')

    with db_engine.connect() as con:
        try:
            con.execute(delete_temp_table_statement)
            logging.info("Deleting temp_component_table...")
            logging.debug('delete_temp_table_statement statement = ' + str(delete_temp_table_statement))
        except Exception as e:
            logging.error("Error deleting the temp_component table from database!")
            logger.error(e, e.args)
            exit(1)


##########################################################################################
# Spectrum file parsing
##########################################################################################

def import_spectrum_from_file(input_df, farm_id):
    # get the date from the first row (column names - wtf)
    sample_date = input_df.columns[1]
    if type(sample_date) != datetime.datetime:
        logging.error("agsource_parser.import_spectrum_from_file(): sample date not registering as datetime.datetime!")
        return False

    # create write_df to hold data from file before writing to database
    db_column_names = ['farm_id', 'sample_date', 't_stamp', 'job_type_name', 'sample_id', 'wave_number', 'min_mu',
                       'max_mu', 'values']
    write_df = pd.DataFrame(columns=db_column_names)

    min_mu = int(input_df.iloc[0][4])
    max_mu = int(input_df.iloc[0][-1])

    # iterate over rows in file
    for row in input_df.iterrows():
        # skip header rows
        if type(row[1].to_list()[2]) == int:
            # convert datetime.time to datetime.datetime
            if type(row[1].to_list()[0]) == datetime.time:
                year = sample_date.year
                month = sample_date.month
                day = sample_date.day
                hour = row[1].to_list()[0].hour
                minute = row[1].to_list()[0].minute
                second = row[1].to_list()[0].second
                timestamp = datetime.datetime(year, month, day, hour, minute, second)
            elif type(row[1].to_list()[0]) == datetime.datetime:
                timestamp = row[1].to_list()[0]
            else:
                logging.info(
                    "agsource_parser.import_spectrum_from_file(): timestamp not registering as datetime or time - inserting sample time to record " + str(
                        row[1].to_list()[2]))
                timestamp = sample_date

            append_spectrum_row(farm_id, timestamp, sample_date, min_mu, max_mu, row[1].to_list(), write_df)

    return write_df


def create_temp_spectrum_table(db_engine, table_name):
    # check and delete if table already exists
    if db_engine.dialect.has_table(db_engine, table_name):
        logger.debug("Deleting old (pre-existing) temp_spectrum table...")
        delete_temp_spectrum_table(db_engine, table_name)

    # create the new table
    create_temp_table_statement = text( \
        'CREATE TABLE ' + table_name + ' ( ' \
                                       'id                  serial primary key, ' \
                                       'farm_id             bigint,' \
                                       'sample_date         timestamp,' \
                                       't_stamp             timestamp,' \
                                       'job_type_name       varchar(255),' \
                                       'sample_id           int,' \
                                       'wave_number         int,' \
                                       'min_mu              int,' \
                                       'max_mu              int,' \
                                       'values              float[1060],' \
                                       'UNIQUE(farm_id, t_stamp, sample_id)' \
                                       ');'
    )

    with db_engine.connect() as con:
        try:
            logger.info("Creating temp_spectrum_table...")
            logger.debug('create_temp_spectrum_table statement = ' + str(create_temp_table_statement))
            con.execute(create_temp_table_statement)
        except Exception as e:
            logger.error("Error creating the temp_spectrum table from database!")
            logger.error(e)
            exit(1)


def append_spectrum_row(farm_id, timestamp, sample_date, min_mu, max_mu, row, write_df):
    # create an array with the data to be appended
    # ['id', 'farm_id', 'sample_date', 't_stamp', 'job_type_name', 'sample_id', 'wave_number', 'min_mu', 'max_mu', 'values']
    try:
        if row[1] == 'Normal':
            new_row = [int(farm_id), sample_date, timestamp, row[1], int(row[2]), int(row[3]), min_mu, max_mu,
                       grab_spectrum_values_as_array(row, min_mu, max_mu)]
        else:
            new_row = [int(farm_id), sample_date, timestamp, row[1], int(row[2]), None, None, None,
                       [None] * (max_mu - min_mu)]

    except Exception as e:
        logging.error("agsource_parser.append_component_row() - exception raised!")
        logger.error(e.args)
        exit()

    logging.debug('appending new spectrum row: ' + str(new_row))
    # append the data to the dataframe in the last position
    write_df.loc[len(write_df)] = new_row


def grab_spectrum_values_as_array(row, min_mu, max_mu):
    values = []
    for i in range(min_mu, max_mu + 1):
        col_number = i - (min_mu - 4)
        values.append(row[col_number])
    return values


def write_spectrum_to_temp(write_df, db_engine, table_name):
    insert_statement = text('INSERT INTO ' + table_name + ' (farm_id, sample_date, t_stamp, job_type_name, sample_id, wave_number, min_mu, max_mu, values) \
        VALUES (:farm_id, :sample_date, :t_stamp, :job_type_name, :sample_id, :wave_number, :min_mu, :max_mu, :values);')

    with db_engine.connect() as con:
        for row in write_df.itertuples():

            data = ({"farm_id": row.farm_id, "sample_date": row.sample_date, "t_stamp": row.t_stamp,
                     "job_type_name": row.job_type_name, "sample_id": row.sample_id, \
                     "wave_number": row.wave_number, "min_mu": row.min_mu, "max_mu": row.max_mu, "values": row.values})

            try:
                logger.debug('insert to temp_spectrum statement = ' + str(insert_statement))
                con.execute(insert_statement, data)

            except Exception as e:
                logger.error("agsource_parser.py - write_spectrum_to_temp(): Error inserting to temp_spectrum table!")
                logger.error(e.args)
                exit(1)


def merge_spectrum_table(db_engine, temp_table_name):
    # all this is mainly to get around the lack of 'ON CONFLICT' in postgres <9.5 - may also be more robust?

    table_name = 'ag_source.spectrum'

    merge_temp_table_statement = text(
        'BEGIN; \
        LOCK TABLE ag_source.spectrum IN EXCLUSIVE MODE; \
        UPDATE {to_table} \
        SET (farm_id, sample_date, t_stamp, job_type_name, sample_id, wave_number, min_mu, max_mu, values) =\
        ({temp_table}.farm_id, {temp_table}.sample_date, {temp_table}.t_stamp, {temp_table}.job_type_name, {temp_table}.sample_id,\
        {temp_table}.wave_number, {temp_table}.min_mu, {temp_table}.max_mu, {temp_table}.values) \
        FROM {temp_table} \
        WHERE {temp_table}.farm_id = {to_table}.farm_id \
        AND {temp_table}.sample_date = {to_table}.sample_date \
        AND {temp_table}.sample_id = {to_table}.sample_id;\
        INSERT INTO {to_table} (farm_id, sample_date, t_stamp, job_type_name, sample_id, wave_number, min_mu, max_mu, values)\
        SELECT {temp_table}.farm_id, {temp_table}.sample_date, {temp_table}.t_stamp, {temp_table}.job_type_name, {temp_table}.sample_id, \
        {temp_table}.wave_number, {temp_table}.min_mu, {temp_table}.max_mu, {temp_table}.values \
        FROM {temp_table} \
        LEFT OUTER JOIN {to_table} ON ( \
        {to_table}.farm_id = {temp_table}.farm_id \
        AND {temp_table}.sample_date = {to_table}.sample_date \
        AND {temp_table}.sample_id = {to_table}.sample_id \
        ) \
        WHERE {to_table}.id IS NULL; \
        COMMIT;'.format(to_table=table_name, temp_table=temp_table_name))

    with db_engine.connect() as con:
        try:
            logging.info("Merging temp_spectrum_table...")
            logging.debug('merge_spectrum_table statement = ' + str(merge_temp_table_statement))
            con.execute(merge_temp_table_statement)
        except Exception as e:
            logging.error("Error merging the temp_spectrum table to the ag_source.spectrum table!")
            logger.error(e.args)
            exit(1)


def delete_temp_spectrum_table(db_engine, table_name):
    delete_temp_table_statement = text('DROP TABLE ' + table_name + ';')

    with db_engine.connect() as con:
        try:
            con.execute(delete_temp_table_statement)
            logging.info("Deleting temp_tspectrum_table...")
            logging.debug('delete_temp_table_statement statement = ' + str(delete_temp_table_statement))
        except Exception as e:
            logging.error("Error deleting the temp_spectrum table from database!")
            logger.error(e.args)
            exit(1)


####################
# HELPER FUNCTIONS #
####################


def headers_checkout(input_df):
    ideal_column_names = ['CNTL', 'BNAME', 'SAMP', 'DIM', 'MILK', 'Fat', 'Tru.Prot', 'Lactose', 'SnF', 'Cells', 'Urea',
                          'MonoUnsaturatedFA', 'PolyUnsaturatedFA', 'SaturatedFA', 'TotalUnsaturatedFA', 'SCFA', 'MCFA',
                          'LCFA', 'Trans FA', 'Aceton', 'BHB', 'C14_0', 'C16_0', 'C18_0', 'C18_1', 'De novo FA',
                          'Mixed FA', 'Preformed FA']

    # get the headers in the order found in the file
    input_column_names = input_df.iloc[0].to_list()

    return input_column_names == ideal_column_names


def get_an_int(input_data):
    if math.isnan(input_data):
        return None
    if type(input_data) == int:
        return input_data
    elif type(input_data) in (str, 'unicode'):
        if input_data == 'nan':
            return None
        else:
            logging.debug("agsource_parser.get_an_int - converting " + type(input_data))
            return int(input_data.replace(',', ''))
    else:
        logging.error('agsource_parser.get_an_int - Error converting ' + str(input_data))
        pdb.set_trace()
        exit(1)


def get_a_float(input_data):
    if type(input_data) == float:
        return input_data
    elif type(input_data) is str:
        if input_data == 'nan':
            return None
        else:
            logging.debug("agsource_parser.get_a_float - converting " + type(input_data))
            return float(input_data.replace(',', ''))
    elif type(input_data) == int:
        return float(input_data)
    else:
        logging.error('agsource_parser.get_a_float - Error converting ' + str(input_data))
        exit(1)


def get_a_date(input_data):
    if type(input_data) == datetime:
        return input_data
    elif type(input_data) is str:
        if input_data == 'nan':
            return None
        else:
            return datetime(input_data)
    else:
        logging.error('agsource_parser.get_a_date - Error converting ' + str(input_data))
        exit(1)


def get_dollar_value(str_value):
    if str_value.lstrip() == '-':
        return None
    else:
        value = float(str_value[1:].encode('ascii'))
        return value
