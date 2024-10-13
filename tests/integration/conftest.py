from pytest import fixture


@fixture(scope="session")
def logger():
    pass
    # DebuggingHook(level=INFO)
