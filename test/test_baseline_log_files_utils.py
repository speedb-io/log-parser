# Copyright (C) 2023 Speedb Ltd. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.'''

import baseline_log_files_utils as baseline_utils
import db_options
import utils

Ver = baseline_utils.Version
baseline_logs_folder_path = "../" + utils.BASELINE_LOGS_FOLDER


def test_version():
    assert str(Ver("1.2.3")) == "1.2.3"
    assert str(Ver("1.2")) == "1.2"
    assert str(Ver("10.20.30")) == "10.20.30"
    assert str(Ver("10.20")) == "10.20"

    v1 = Ver("1.2.3")
    assert v1.major == 1
    assert v1.minor == 2
    assert v1.patch == 3

    v2 = Ver("4.5")
    assert v2.major == 4
    assert v2.minor == 5
    assert v2.patch is None

    assert Ver("1.2.3") < Ver("2.1.1")
    assert Ver("2.1.1") > Ver("1.2.3")
    assert Ver("2.2.3") < Ver("2.3.1")
    assert Ver("2.2.1") < Ver("2.2.2")
    assert not Ver("2.2") < Ver("2.2")
    assert Ver("2.2") < Ver("2.2.0")
    assert Ver("2.2") < Ver("2.3")
    assert Ver("2.2") < Ver("2.2.2")
    assert Ver("2.2.1") > Ver("2.2")

    assert Ver("2.2.3") == Ver("2.2.3")
    assert Ver("2.2") == Ver("2.2")
    assert Ver("2.2.3") != Ver("2.2")


def test_find_closest_version():
    # prepare an unsorted list on purpose
    baseline_versions = [Ver("1.2"),
                         Ver("1.2.0"),
                         Ver("1.2.1"),
                         Ver("2.1"),
                         Ver("2.1.0"),
                         Ver("3.0"),
                         Ver("3.0.0")]

    find = baseline_utils.find_closest_version_idx

    assert find(baseline_versions, Ver("1.1")) is None
    assert find(baseline_versions, Ver("1.1.9")) is None
    assert find(baseline_versions, Ver("1.2")) == 0  # Ver("1.2")
    assert find(baseline_versions, Ver("1.2.0")) == 1  # Ver("1.2.0")
    assert find(baseline_versions, Ver("1.2.2")) == 2  # Ver("1.2.1")
    assert find(baseline_versions, Ver("2.0.0")) == 2  # Ver("1.2.1")
    assert find(baseline_versions, Ver("2.1")) == 3  # Ver("2.1")
    assert find(baseline_versions, Ver("2.1.0")) == 4  # Ver("2.1.0")
    assert find(baseline_versions, Ver("2.1.1")) == 4  # Ver("2.1.0")
    assert find(baseline_versions, Ver("3.0")) == 5  # Ver("3.0")
    assert find(baseline_versions, Ver("3.0.0")) == 6  # Ver("3.0.0")
    assert find(baseline_versions, Ver("3.0.1")) == 6  # Ver("3.0.0")
    assert find(baseline_versions, Ver("3.1")) == 6  # Ver("3.0.0")
    assert find(baseline_versions, Ver("99.99")) == 6  # Ver("3.0.0")
    assert find(baseline_versions, Ver("99.99.99")) == 6  # Ver("3.0.0")


def test_find_closest_baseline_log_file():
    find = baseline_utils.find_closest_baseline_info

    versions_to_test_info = [
        ("1.0.0", utils.ProductName.SPEEDB, None, None),
        ("2.2.1", utils.ProductName.SPEEDB, "LOG-speedb-2.2.1",
         "2.2.1"),
        ("2.2.2", utils.ProductName.SPEEDB, "LOG-speedb-2.2.1",
         "2.2.1"),
        ("32.0.1", utils.ProductName.SPEEDB, "LOG-speedb-2.5.0",
         "2.5.0"),
        ("6.0.0", utils.ProductName.ROCKSDB, None, None),
        ("6.26.0", utils.ProductName.ROCKSDB,
         "LOG-rocksdb-6.26.0", "6.26.0"),
        ("6.27.0", utils.ProductName.ROCKSDB,
         "LOG-rocksdb-6.26.0", "6.26.0"),
        ("7.10.11", utils.ProductName.ROCKSDB,
         "LOG-rocksdb-7.9.2", "7.9.2"),
        ("8.0.0", utils.ProductName.ROCKSDB,
         "LOG-rocksdb-8.0.0", "8.0.0"),
        ("9.0.0", utils.ProductName.ROCKSDB,
         "LOG-rocksdb-8.5.3", "8.5.3")
    ]

    for version_info in versions_to_test_info:
        closest_baseline_info = \
            find(baseline_logs_folder_path,
                 version_info[1],
                 Ver(version_info[0]))
        if version_info[2] is None:
            assert closest_baseline_info is None
        else:
            assert closest_baseline_info is not None
            assert closest_baseline_info.file_path.name == version_info[2]
            assert closest_baseline_info.version == Ver(version_info[3])


def test_find_options_diff():
    baseline_info_rocksdb_7_7_3 = \
        baseline_utils.get_baseline_database_options(
            baseline_logs_folder_path,
            utils.ProductName.ROCKSDB,
            version_str="7.7.3")
    assert isinstance(baseline_info_rocksdb_7_7_3,
                      baseline_utils.BaselineDBOptionsInfo)

    database_options_rocksdb_7_7_3 = \
        baseline_info_rocksdb_7_7_3.baseline_options

    diff_rocksdb_7_7_3_vs_itself = \
        baseline_utils.find_options_diff_relative_to_baseline(
            baseline_logs_folder_path,
            utils.ProductName.ROCKSDB,
            Ver("7.7.3"),
            database_options_rocksdb_7_7_3)
    assert isinstance(diff_rocksdb_7_7_3_vs_itself,
                      baseline_utils.OptionsDiffRelToBaselineInfo)
    assert diff_rocksdb_7_7_3_vs_itself.diff is None
    assert diff_rocksdb_7_7_3_vs_itself.closest_version == Ver("7.7.3")

    baseline_info_rocksdb_2_1_0 = \
        baseline_utils.get_baseline_database_options(
            baseline_logs_folder_path,
            utils.ProductName.SPEEDB,
            version_str="2.1.0")

    database_options_speedb_2_1_0 = \
        baseline_info_rocksdb_2_1_0.baseline_options
    diff_speedb_2_1_0_vs_itself = \
        baseline_utils.find_options_diff_relative_to_baseline(
            baseline_logs_folder_path,
            utils.ProductName.SPEEDB,
            Ver("2.1.0"),
            database_options_speedb_2_1_0)
    assert diff_speedb_2_1_0_vs_itself.diff is None
    assert diff_speedb_2_1_0_vs_itself.closest_version == Ver("2.1.0")

    expected_diff = {}
    updated_database_options_2_1_0 = database_options_speedb_2_1_0

    curr_max_open_files_value = \
        updated_database_options_2_1_0.get_db_wide_option('max_open_files')
    new_max_open_files_value = str(int(curr_max_open_files_value) + 100)
    updated_database_options_2_1_0.set_db_wide_option('max_open_files',
                                                      new_max_open_files_value)
    expected_diff['DBOptions.max_open_files'] = \
        {utils.NO_CF: (curr_max_open_files_value,
                       new_max_open_files_value)}

    updated_database_options_2_1_0.set_db_wide_option("NEW_DB_WIDE_OPTION1",
                                                      "NEW_DB_WIDE_VALUE1",
                                                      True)
    expected_diff['DBOptions.NEW_DB_WIDE_OPTION1'] = \
        {utils.NO_CF: (db_options.SANITIZED_NO_VALUE, "NEW_DB_WIDE_VALUE1")}

    cf_name = "default"

    curr_ttl_value = updated_database_options_2_1_0.get_cf_option(cf_name,
                                                                  "ttl")
    new_ttl_value = str(int(curr_ttl_value) + 1000)
    updated_database_options_2_1_0.set_cf_option(cf_name, "ttl",
                                                 new_ttl_value)
    updated_database_options_2_1_0.set_cf_option('default', 'ttl',
                                                 new_ttl_value)
    expected_diff['CFOptions.ttl'] = {'default': (curr_ttl_value,
                                                  new_ttl_value)}

    updated_database_options_2_1_0.set_cf_option(cf_name, "NEW_CF_OPTION1",
                                                 "NEW_CF_VALUE1", "True")
    expected_diff['CFOptions.NEW_CF_OPTION1'] =\
        {'default': (db_options.SANITIZED_NO_VALUE, "NEW_CF_VALUE1")}

    curr_block_align_value = \
        updated_database_options_2_1_0.get_cf_table_option(cf_name,
                                                           "block_align")
    new_block_align_value = "dummy_block_align_value"
    updated_database_options_2_1_0.set_cf_table_option(cf_name,
                                                       "block_align",
                                                       new_block_align_value)
    expected_diff['TableOptions.BlockBasedTable.block_align'] = \
        {'default': (curr_block_align_value, new_block_align_value)}
    updated_database_options_2_1_0.set_cf_table_option(cf_name,
                                                       "NEW_CF_TABLE_OPTION1",
                                                       "NEW_CF_TABLE_VALUE1",
                                                       "True")
    expected_diff['TableOptions.BlockBasedTable.NEW_CF_TABLE_OPTION1'] = \
        {'default': (db_options.SANITIZED_NO_VALUE, "NEW_CF_TABLE_VALUE1")}

    diff_speedb_2_1_0_vs_itself = \
        baseline_utils.find_options_diff_relative_to_baseline(
            baseline_logs_folder_path,
            utils.ProductName.SPEEDB,
            Ver("2.1.0"),
            updated_database_options_2_1_0)
    assert diff_speedb_2_1_0_vs_itself.diff.get_diff_dict() == expected_diff
    assert diff_speedb_2_1_0_vs_itself.closest_version == Ver("2.1.0")
