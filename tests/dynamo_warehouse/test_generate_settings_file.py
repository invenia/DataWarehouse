from unittest.mock import Mock, patch

import pytest

from datawarehouse.implementations import (
    MIN_BACKEND_VERSION,
    generate_settings_file,
)


@pytest.fixture()
def patcher():
    # mock path
    path_patcher = patch("datawarehouse.implementations.Path")
    patched_path = path_patcher.start()

    # mock stack output
    stack_patcher = patch("datawarehouse.implementations.get_stack_output")
    patched_stack = stack_patcher.start()

    yield patched_path, patched_stack
    path_patcher.stop()
    stack_patcher.stop()


def _gen_output(version: str = "v2.0.0"):
    return {
        "RegionName": "test-region",
        "RegistryTable": "test-registry-table",
        "SourceDataTable": "test-source-table",
        "SourceBucket": "test-source-bucket",
        "ParsedBucket": "test-parsed-bucket",
        "StoragePrefix": "test-prefix",
        "StackVersion": version,
    }


def test_backend_version(patcher):
    patched_path, patched_stack = patcher

    mocked_path = Mock()
    mocked_path.is_file.return_value = False
    patched_path.return_value = mocked_path

    # backend_version == MIN_BACKEND_VERSION
    configs = _gen_output(MIN_BACKEND_VERSION)
    patched_stack.return_value = configs
    generate_settings_file("test-backend")
    assert mocked_path.write_text.call_count == 1

    # backend_version > MIN_BACKEND_VERSION
    configs = _gen_output("v9999.9.9")
    patched_stack.return_value = configs
    generate_settings_file("test-backend")
    assert mocked_path.write_text.call_count == 2

    # backend_version < MIN_BACKEND_VERSION
    configs = _gen_output("v0.0.1")
    patched_stack.return_value = configs
    with pytest.raises(Exception):
        generate_settings_file("test-backend")
