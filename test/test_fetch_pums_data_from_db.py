from __future__ import (
    absolute_import, division, print_function, unicode_literals
)
import unittest
import mock
from doppelganger.scripts.fetch_pums_data_from_db import link_fields_to_inputs, fetch_pums_data
from doppelganger import inputs, config


class TestFetchPumsDataFromDB(unittest.TestCase):

    def _mock_legit_person_input_list(self):
        return ['age', 'sex', 'individual_income']

    def _mock_illegit_person_input_list(self):
        return ['not_a_field', 'sex', 'individual_income']

    def test_link_fields_to_inputs(self):
        self.assertEqual(set([inputs.AGE, inputs.SEX, inputs.INDIVIDUAL_INCOME]),
                         link_fields_to_inputs(self._mock_legit_person_input_list()))
        self.assertRaises(ValueError, link_fields_to_inputs, self._mock_illegit_person_input_list())

    @mock.patch('doppelganger.scripts.fetch_pums_data_from_db.psycopg2')
    @mock.patch('doppelganger.scripts.fetch_pums_data_from_db.datasource.PumsData')
    def test_fetch_pums_data(self, mock_PumsData, mock_psycopg2):
        mock_config = config.Configuration.from_json(self._mock_config())
        persons_data, households_data =\
            fetch_pums_data(state_id='01', puma_id='00001', configuration=mock_config,
                            db_host='host1', db_database='db1', db_schema='schema1',
                            db_user='user1', db_password='password1')
        mock_psycopg2.connect.assert_called()
        mock_PumsData.from_database.assert_called()
