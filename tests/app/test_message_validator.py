import pytest
import json

from app.message_validator import MessageValidator


def test_validate_message_valid():
    # Create the message validator we'll be testing against
    message_validator = MessageValidator('3.0.1')

    # Get a handle on the test JSON message
    test_message = _get_test_message('tests/app/data/rdss-message.json')

    # Validate the message
    errors = message_validator.message_errors(test_message)

    assert len(errors) == 0


def test_validate_message_invalid():
    # Create the message validator we'll be testing against
    message_validator = MessageValidator('3.0.1')

    # Get a handle on the test JSON message
    test_message = _get_test_message('tests/app/data/rdss-message-invalid.json')

    # Validate the message
    errors = message_validator.message_errors(test_message)

    assert len(errors) == 1

def _get_test_message(file_path):
    with open(file_path, 'rb') as f_in:
        return json.load(f_in)
