import pytest

from app import MessageValidator
from jsonschema import ValidationError


def test_validate_message_valid():
    # Create the message validator we'll be testing against
    message_validator = MessageValidator('3.0.1')

    # Get a handle on the test JSON message
    test_message = _get_test_message('tests/app/data/rdss-message.json')

    # Validate the message
    message_validator.validate_message(test_message)


def test_validate_message_invalid():
    # Validate that this call to raises a ValidationError
    with pytest.raises(ValidationError):
        # Create the message validator we'll be testing against
        message_validator = MessageValidator('3.0.1')

        # Get a handle on the test JSON message
        test_message = _get_test_message('tests/app/data/rdss-message-invalid.json')

        # Validate the message
        message_validator.validate_message(test_message)


def _get_test_message(file_path):
    with open(file_path, 'rb') as file:
        return file.read()
