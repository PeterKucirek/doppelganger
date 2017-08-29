from __future__ import (
    absolute_import, division, print_function, unicode_literals
)
import argparse
import csv
import os
import pandas as pd
from doppelganger import (
    inputs,
    datasource,
    Configuration,
    HouseholdAllocator,
    SegmentedData,
    BayesianNetworkModel,
    Population,
    Marginals,
)
import fetch_pums_data_from_db

FILE_PATTERN = 'state_{}_puma_{}_{}'


def parse_args():
    '''Allow the parsing of command line parameters to this program. Essentially establishes an
    interface for other programs to use doppelganger to generate synthetic populations.
    Args:
        None
    Returns:
        A dictionary of parsed arguments.
        Ex. {'config_file': './config.json', 'state_id':'06', 'puma_id': '00106'}
    '''
    parser = argparse.ArgumentParser(
        '''Run doppelganger on a state and puma

        1. Download pums data from a postgres database if necessary.
            A 'person' and 'household' table is assumed, and must share fields with the
            PUMS 2015 1-year product.
            https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMSDataDict15.pdf
            Skipped if data is already found.
        2. Fetch census marginal (currently tract-level) data using a Census API key.
        3. Generate population and households

        '''
    )
    parser.add_argument('--puma_tract_mappings_csv', type=lambda x: is_valid_file(parser, x),
                        help='csv with (state, county, tract, puma)',
                        default='./examples/sample_data/2010_puma_tract_mapping.txt')
    parser.add_argument('--config_file', type=lambda x: is_valid_file(parser, x),
                        help='file to load configuration from. \
                        see examples/sample_data/config.json for an example',
                        default='./examples/sample_data/config.json')
    parser.add_argument('--state_id', type=unicode,
                        help='state code of area to fetch marginals for',
                        default='06')
    parser.add_argument('--puma_id', type=unicode,
                        help='puma code of area to fetch marginals for',
                        default='00106')
    parser.add_argument('--census_api_key', type=unicode,
                        help='key used to download marginal data from the census'
                        'http://api.census.gov/data/key_signup.html',
                        default='')
    parser.add_argument('--output_dir', type=lambda x: is_valid_file(parser, x),
                        help='path for output csv', default='.')
    parser.add_argument('--db_host', type=unicode,
                        help='hostname of database with pums data', default='localhost')
    parser.add_argument('--db_database', type=unicode, help='db name')
    parser.add_argument('--db_schema', type=unicode, help='db schema', default='import')
    parser.add_argument('--db_user', type=unicode, help='db user', default='postgres')
    parser.add_argument('--db_password', type=unicode, help='db password')
    return parser.parse_args()


def download_and_load_pums_data(
        output_dir, state_id, puma_id, configuration,
        db_host, db_database, db_schema, db_user, db_password
        ):
    '''Do the pums files already exist --
            if no - read from db, write csv; load the csv
            if yes - load csv file

    Args:
        output_dir: place to look for and write pums household and person data files
        state_id: 2-digit state fips code
        puma_id: 5-digit puma code
        configuration: keeps track of which variables/models belong to households and persons
        db_host: hostname of the POSTGRESQL instance to connect to
        db_database: database name to connect to
        db_schema: schema which _must_ contain a person and household table with pums fields
            referenced in doppelganger/inputs.py
        db_user: username to connect with
        db_password: password to authenticate to the database
    Returns:
        Household and Person dataframes with the pums fields specified above.
    '''
    household_filename = FILE_PATTERN.format(state_id, puma_id, 'households_pums.csv')
    household_path = os.path.sep.join([output_dir, household_filename])
    person_filename = FILE_PATTERN.format(state_id, puma_id, 'persons_pums.csv')
    person_path = os.path.sep.join([output_dir, person_filename])

    if not os.path.exists(household_path) or not os.path.exists(person_path):
        print('Data not found at: \n{}\n or {}. Downloading data from the db'
              .format(household_path, person_path))
        households_data, persons_data = fetch_pums_data_from_db.fetch_pums_data(
                state_id, puma_id, configuration,
                db_host, db_database, db_schema, db_user, db_password,
            )
        # Write data to files, so mustn't be downloaded again
        households_data.data.to_csv(household_path)
        persons_data.data.to_csv(person_path)

    households_data = datasource.CleanedData.from_csv(household_path)
    persons_data = datasource.CleanedData.from_csv(person_path)

    return households_data, persons_data


def create_bayes_net(state_id, puma_id, output_dir, households_data, persons_data, configuration):
    '''Create a bayes net from pums dataframes and a configuration.
    Args:
        state_id: 2-digit state fips code
        puma_id: 5-digit puma code
        output_dir: dir to write out the generated bayesian nets to
        households_data: pums households data frame
        persons_data: pums persons data frame
        configuration: specifies the structure of the bayes net
    Returns:
        household and person bayesian models
        TODO allow custom segmentation functions to be passed into this function
    '''
    # Person Network with Age Segmentation
    def person_segmentation(x): return x[inputs.AGE.name]

    # Write the persons bayes net to disk
    person_training_data = SegmentedData.from_data(
        persons_data,
        list(configuration.person_fields),
        inputs.PERSON_WEIGHT.name,
        person_segmentation
    )
    person_model = BayesianNetworkModel.train(
        person_training_data,
        configuration.person_structure,
        configuration.person_fields
    )

    person_model_filename = os.path.join(
                output_dir, FILE_PATTERN.format(state_id, puma_id, 'person_model.json')
            )
    person_model.write(person_model_filename)

    # Write the household bayes net to disk
    def household_segmenter(x): x[inputs.NUM_PEOPLE.name]

    household_training_data = SegmentedData.from_data(
        households_data,
        list(configuration.household_fields),
        inputs.HOUSEHOLD_WEIGHT.name,
        household_segmenter,
    )
    household_model = BayesianNetworkModel.train(
        household_training_data,
        configuration.household_structure,
        configuration.household_fields
    )

    household_model_filename = os.path.join(
                output_dir, FILE_PATTERN.format(state_id, puma_id, 'household_model.json')
            )
    household_model.write(household_model_filename)
    return household_model, person_model


class CensusFetchException(Exception):
    pass


def download_tract_data(state_id, puma_id, output_dir, census_api_key, puma_tract_mappings,
                        households_data, persons_data):
    '''Download tract data from the US Census' API.
    Initilize an allocator, capable of allocating PUMS households as best as possible based on
    marginal census (currently tract) data using a cvx-solver.

    Args:
        state_id: 2-digit state fips code
        puma_id: 5-digit puma code
        output_dir: dir to write out the generated bayesian nets to
        households_data: pums households data frame
        persons_data: pums persons data frame
        configuration: specifies the structure of the bayes net

    Returns:
        An allocator described above.
    '''

    marginal_filename = os.path.join(
                output_dir, FILE_PATTERN.format(state_id, puma_id, 'marginals.csv')
            )

    try:  # Already have marginals file
        controls = Marginals.from_csv(marginal_filename)
    except:  # Download marginal data from the Census API
        with open(puma_tract_mappings) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            marginals = Marginals.from_census_data(
                csv_reader, census_api_key, state=state_id, pumas=puma_id
            )
            if len(marginals.data) <= 1:
                raise CensusFetchException()
            else:
                marginals.write(marginal_filename)
        controls = Marginals.from_csv(marginal_filename)

    '''With the above marginal controls, the methods in allocation.py allocate discrete PUMS
    households to the subject PUMA.'''

    allocator = HouseholdAllocator.from_cleaned_data(controls, households_data, persons_data)
    return allocator


def generate_synthetic_people_and_households(state_id, puma_id, output_dir, allocator,
                                             person_model, household_model):
    '''Replace the PUMS Persons with Synthetic Persons created from the Bayesian Network.
       Writes out a combined person-household dataframe.
    Args:
        state_id: 2-digit state fips code
        puma_id: 5-digit puma code
        allocator: PUMS households as best as possible based on marginal census (currently tract)
            data using a cvx-solver.
        person_model: bayesian model describing the discritized pums fields' relation to one another
        household_model: same as person_model but for households
    Returns:
        combined: joined pandas dataframe of generated households and persons
    '''
    population = Population.generate(allocator, person_model, household_model)
    people = population.generated_people
    households = population.generated_households

    '''To create one fat table of people and household attributes we can join on\
    tract, serial_number, and repeat_index:'''

    merge_cols = ['tract', 'serial_number', 'repeat_index']
    combined = pd.merge(people, households, on=merge_cols)
    combined.to_csv(os.path.join(
            output_dir, FILE_PATTERN.format(state_id, puma_id, 'generated.csv')
        ))
    return combined


def is_valid_file(parser, filename):
    '''Convenience function to validate files passed into the argument parser
    Args:
        parser: a python argument parser
        filename: name of file to validate
    Returns: filename
    Raises: ParserError
    '''
    if not os.path.exists(filename):
        parser.error("The file %s does not exist!" % filename)
    else:
        return filename


def main():
    args = parse_args()
    puma_tract_mappings = args.puma_tract_mappings_csv
    state_id = args.state_id
    puma_id = args.puma_id
    census_api_key = args.census_api_key
    config_file = args.config_file
    output_dir = args.output_dir
    db_host = args.db_host
    db_database = args.db_database
    db_schema = args.db_schema
    db_user = args.db_user
    db_password = args.db_password

    configuration = Configuration.from_file(config_file)

    households_data, persons_data = download_and_load_pums_data(
                output_dir, state_id, puma_id,
                configuration, db_host, db_database, db_schema, db_user, db_password
            )

    household_model, person_model = create_bayes_net(
                state_id, puma_id, output_dir,
                households_data, persons_data, configuration
            )

    allocator = download_tract_data(
                state_id, puma_id, output_dir, census_api_key, puma_tract_mappings,
                households_data, persons_data
            )

    generate_synthetic_people_and_households(
                state_id, puma_id, output_dir, allocator,
                person_model, household_model
            )


if __name__ == '__main__':
    main()
