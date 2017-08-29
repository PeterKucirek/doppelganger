from __future__ import (
    absolute_import, division, print_function, unicode_literals
)
import unittest
from mock import Mock, MagicMock, patch
from doppelganger.scripts.fetch_pums_data_from_db import link_fields_to_inputs, fetch_pums_data
from doppelganger import inputs, datasource


class TestFetchPumsDataFromDB(unittest.TestCase):

    def _mock_params(self):
        return {'state_id': '01', 'puma_id': '00001', 'db_schema': 'schema1', 'db_table': 'table1'}

    def _mock_legit_person_input_list(self):
        return ['age', 'sex', 'individual_income']

    def _mock_illegit_person_input_list(self):
        return ['not_a_field', 'sex', 'individual_income']

    def test_link_fields_to_inputs(self):
        self.assertEqual([inputs.AGE, inputs.SEX, inputs.INDIVIDUAL_INCOME],
                         link_fields_to_inputs(self._mock_legit_person_input_list()))
        self.assertRaises(ValueError, link_fields_to_inputs, self._mock_illegit_person_input_list())

    def test_fetch_pums_data(self):
        from_database = MagicMock()
        with patch('doppelganger.datasource.PumsData.from_database', from_database):
            persons_data, households_data =\
                fetch_pums_data(state_id='01', puma_id='00001', configuration=MagicMock(),
                                db_host='host1', db_database='db1', db_schema='schema1',
                                db_user='user1', db_password='password1')
        from_database.assert_called_once_with(
                puma_conn=Mock(), 
                state_id='01', puma_id='00001', db_schema='schema1',
                db_table='persons', fields=self._mock_legit_person_input_list()
            )
