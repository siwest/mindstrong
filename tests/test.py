import os
import unittest
from mock import patch, MagicMock


class TestValidator(unittest.TestCase):
    def setUp(self):
        print("\nRunning test of Validator.")

    @patch("validations.validator.Validator")
    def test_validator(self, mock_validator):
        """Test the functionality of the validator

        :param mock_validator: Mock for `validations.validator.Validator` class
        :type mock_validator: unittest.mock.MagicMock
        """

        mocked_validator = mock_validator()

        schema_validator = mocked_validator(MagicMock())

        schema_validator.check_header(MagicMock())
        schema_validator.check_type(MagicMock(), MagicMock())
        schema_validator.check_record_length(MagicMock(), MagicMock())

        mocked_validator.assert_called_once()
        print(
            "Asserted the class instance is created once despite many function calls."
        )

    def tearDown(self):
        print("Done with test on Validator.")


if __name__ == "__main__":
    unittest.main()
