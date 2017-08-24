import argparse
import csv
import os
import pandas as pd
from doppelganger import (
    allocation,
    inputs,
    Configuration,
    HouseholdAllocator,
    PumsData,
    SegmentedData,
    BayesianNetworkModel,
    Population,
    Preprocessor,
    Marginals
)
import fetch_puma_data_from_db


def load_config(configuration):
    '''Load the Doppelganger configuration file with the preprocessor object that creates
    methods to apply to the household and person PUMS data.'''
    return Preprocessor.from_config(configuration.preprocessing_config)


def download_and_load_pums_data(
        output_dir, state_id, puma_id,
        preprocessor, configuration,
        db_host, db_database, db_schema, db_user, db_password,
        extra_person_fields, extra_household_fields
        ):
    '''Does the file pums files already exist --
            if no - read from db, write csv; load the csv
            if yes - load csv file
    '''
    household_filename = 'state_{}_puma_{}_households_data.csv'.format(state_id, puma_id)
    household_path = os.path.sep.join([output_dir, household_filename])
    person_filename = 'state_{}_puma_{}_persons_data.csv'.format(state_id, puma_id)
    person_path = os.path.sep.join([output_dir, person_filename])

    ''' Person Data
    The allocation.DEFAULT_PERSON_FIELDS defines a set of fields that can be used to create the
    persons_data; we take the union of these defaults with those defined in the person_categories
    section of the configuration file. This data is then extracted from the raw/dirty PUMS data.
    '''
    if not os.path.exists(household_path) or not os.path.exists(person_path):
        print 'Downloading data from the db'
        fetch_puma_data_from_db.fetch_data(
                output_dir, state_id, puma_id, db_host, db_database,
                db_schema, db_user, db_password,
                extra_person_fields, extra_household_fields
                )

    household_fields = tuple(set(
        field.name for field in allocation.DEFAULT_HOUSEHOLD_FIELDS).union(
            set(configuration.household_fields)
    ))

    households_data = PumsData.from_csv(household_path).clean(
            household_fields, preprocessor, puma=puma_id)

    persons_fields = tuple(set(
        field.name for field in allocation.DEFAULT_PERSON_FIELDS).union(
            set(configuration.person_fields)
    ))

    persons_data = PumsData.from_csv(os.path.sep.join(
            [output_dir, person_filename])).clean(persons_fields, preprocessor, puma=puma_id)
    # TODO add state filter?

    return households_data, persons_data


def create_bayes_net(state_id, puma_id, output_dir, households_data, persons_data, configuration):
    # Person Network with Age Segmentation
    def person_segmentation(x): return x[inputs.AGE.name]

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

    # The Bayesian Network can be written to disk and read from disk as follows.

    person_model_filename = os.path.join(
                output_dir, 'state_{}_puma_{}_person_model.json'.format(state_id, puma_id)
            )
    person_model.write(person_model_filename)

    # Following the same steps as above, you can also build a household network.

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
                output_dir, 'state_{}_puma_{}_household_model.json'.format(state_id, puma_id)
            )
    household_model.write(household_model_filename)
    return household_model, person_model


def allocate(state_id, puma_id, output_dir, census_api_key, puma_tract_mappings,
             households_data, persons_data):
    # Allocate PUMS households to the PUMA

    marginal_filename = os.path.join(
                output_dir, 'state_{}_puma_{}_marginals.csv'.format(state_id, puma_id)
            )

    try:  # Already have marginals file
        controls = Marginals.from_csv(marginal_filename)
    except:  # Download marginal data from the Census API
        with open(puma_tract_mappings) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            marginals = Marginals.from_census_data(
                csv_reader, census_api_key, state=state_id, puma=puma_id
            )
            marginals.write(marginal_filename)
        controls = Marginals.from_csv(marginal_filename)

    '''With the above marginal controls, the methods in allocation.py allocate discrete PUMS
    households to the subject PUMA.'''

    allocator = HouseholdAllocator.from_cleaned_data(controls, households_data, persons_data)
    return allocator


def generate_synthetic_people_and_households(state_id, puma_id, output_dir, allocator,
                                             person_model, household_model):
    # Replace the PUMS Persons with Synthetic Persons created from the Bayesian Network
    population = Population.generate(allocator, person_model, household_model)
    people = population.generated_people
    households = population.generated_households

    '''To create one fat table of people and household attributes we can join on\
    tract, serial_number, and repeat_index:'''

    merge_cols = ['tract', 'serial_number', 'repeat_index']
    combined = pd.merge(people, households, on=merge_cols)
    combined.to_csv(os.path.join(
            output_dir, 'state_{}_puma_{}_generated.csv'.format(state_id, puma_id)
        ))


def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return arg


def parse_args():
    parser = argparse.ArgumentParser('Fetch census marginal data, and generate population.')
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
    parser.add_argument('--extra_person_fields', type=unicode,
                        help='comma separated list of person pums fields defined in inputs.py',
                        default='individual_income')
    parser.add_argument('--extra_household_fields', type=unicode,
                        help='comma separated list of household pums fields defined in inputs.py',
                        default='household_income,num_vehicles')
    return parser.parse_args()


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
    extra_person_fields = args.extra_person_fields
    extra_household_fields = args.extra_household_fields

    configuration = Configuration.from_file(config_file)

    preprocessor = load_config(configuration)

    households_data, persons_data = download_and_load_pums_data(
                output_dir, state_id, puma_id,
                preprocessor, configuration, db_host, db_database, db_schema, db_user, db_password,
                extra_person_fields, extra_household_fields
            )

    household_model, person_model = create_bayes_net(
                state_id, puma_id, output_dir,
                households_data, persons_data, configuration
            )

    allocator = allocate(
                state_id, puma_id, output_dir, census_api_key, puma_tract_mappings,
                households_data, persons_data
            )

    generate_synthetic_people_and_households(
                state_id, puma_id, output_dir, allocator,
                person_model, household_model
            )


if __name__ == '__main__':
    main()
