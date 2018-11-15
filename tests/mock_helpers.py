import mock
import moto
import contextlib
import functools

def mock_oai_pmh_list_records():
    return 


def mock_oai_pmh_adaptor_infra(oai_pmh_provider):
    mocking_managers = [
        (moto.mock_dynamodb2, [], {}),
        (moto.mock_kinesis, [], {}),
        (moto.mock_s3, [], {}),
        (
            mock.patch,
            ['oaipmh.client.Client.listRecords'],
            {'side_effect': mock_oai_pmh_list_records()},
        ),
    ]

    def decorator(func, *args, **kwargs):
        # `wraps` preserves function info for decorated function e.g. __name__
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # This allows the setup of multiple context managers without lots of nested `withs`
            with contextlib.ExitStack() as stack:
                [
                    stack.enter_context(f(*f_args, **f_kwargs))
                    for f, f_args, f_kwargs in mocking_managers
                ]
                setup_ssm()
                setup_s3()
                return func(*args, **kwargs)
        return wrapper
    return decorator
