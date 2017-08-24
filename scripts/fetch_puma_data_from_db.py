#!/usr/bin/env python2.7
'''Download pums data from a db to speed up run-times.'''

from __future__ import (
    absolute_import, division, print_function, unicode_literals
)
import argparse
import os
import psycopg2
from doppelganger import datasource, allocation, inputs

PERSONS_OUTFILE_PATTERN = 'state_{}_puma_{}_persons_data.csv'
HOUSEHOLDS_OUTFILE_PATTERN = 'state_{}_puma_{}_households_data.csv'

HOUSEHOLD_TABLE = 'households'
PERSONS_TABLE = 'persons'


def fetch_data(output_dir, state_id, puma_id, db_host, db_database, db_schema, db_user, db_password,
               extra_person_fields, extra_household_fields):
    '''Download PUMS data from pums tables stored in a database
    NOTE:
    extra_person_fields (list(unicode)) - must be pre-defined in doppelganger/inputs.py
    extra_household_fields (list(unicode)) - must be pre-defined in doppelganger/inputs.py
    '''

    extra_person_fields = validate_extra_inputs(extra_person_fields.split(','))
    extra_household_fields = validate_extra_inputs(extra_household_fields.split(','))
    puma_conn = None
    try:
        puma_conn = psycopg2.connect(
            host=db_host,
            database=db_database,
            user=db_user,
            password=db_password,
        )

        def filename(output_dir, outfile_pattern, state_id, puma_id):
            return os.path.join(output_dir, outfile_pattern.format(state_id, puma_id))

        persons_data = datasource.PumsData.from_database(
            puma_conn, state_id, puma_id, db_schema, PERSONS_TABLE,
            allocation.DEFAULT_PERSON_FIELDS.union(set(extra_person_fields))
        )
        persons_file = filename(output_dir, PERSONS_OUTFILE_PATTERN, state_id, puma_id)
        persons_data.write(persons_file)

        households_data = datasource.PumsData.from_database(
                puma_conn, state_id, puma_id, db_schema, HOUSEHOLD_TABLE,
                allocation.DEFAULT_HOUSEHOLD_FIELDS.union(set(extra_household_fields))
            )
        household_file = filename(output_dir, HOUSEHOLDS_OUTFILE_PATTERN, state_id, puma_id)
        households_data.write(household_file)

    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        if puma_conn is not None:
            puma_conn.close()
            print('Database connection closed.')


def validate_extra_inputs(input_list):
    '''Verify extra field references have been properly registered in inputs.py
    Map them to their inputs data-types based on their name property.

    Args
    input_list - list of variable names

    Return
    list of variable objects for use in PumsData.from_database
    '''
    input_name_map = {x.name: x for x in inputs.PUMS_INPUTS}
    if not all(x in input_name_map.keys() for x in input_list):
        raise ValueError('One or more extra fields not registered in doppelganger/inputs.py')
    return [input_name_map[i] for i in input_list]


def main():
    parser = argparse.ArgumentParser('Fetch puma persons and households')
    parser.add_argument('--state', type=unicode, help='state to fetch data for', default='29')
    parser.add_argument('--puma', type=unicode, help='puma to fetch data for', default='00901')
    parser.add_argument('--output_dir', type=unicode, help='path to directory of output files',
                        default='.')
    parser.add_argument('--db_host', type=unicode,
                        help='hostname of database with pums data',
                        default='localhost')
    parser.add_argument('--db_database', type=unicode, help='db name', default='localhost')
    parser.add_argument('--db_schema', type=unicode, help='db schema', default='import')
    parser.add_argument('--db_user', type=unicode, help='db user', default='localhost')
    parser.add_argument('--db_password', type=unicode, help='db password', default='localhost')
    parser.add_argument('--extra_person_fields', type=unicode,
                        help='comma-separated list of additional (lowercase) input person pums \
                        variables. must be pre-defined in doppelganger/inputs.py',
                        default=['individual_income'])
    parser.add_argument('--extra_household_fields', type=unicode,
                        help='comma-separated list of additional (lowercase) input person pums \
                        variables. must be pre-defined in doppelganger/inputs.py',
                        default=['household_income', 'num_vehicles'])
    args = parser.parse_args()

    extra_person_fields = args.extra_person_fields
    extra_household_fields = args.extra_household_fields

    fetch_data(
                os.path.expanduser(args.output_dir), args.state, args.puma,
                args.db_host, args.db_schema, args.db_user, args.db_password,
                extra_person_fields, extra_household_fields
            )


if __name__ == '__main__':
    main()
