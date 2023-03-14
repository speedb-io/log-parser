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

import test.testing_utils as test_utils
from cfs_infos import CfMetadata, CfsMetadata, CfDiscoveryType

default = "default"
cf1 = "cf1"
cf2 = "cf2"
cf3 = "cf3"
cf1_id = 10
cf2_id = 20

cf1_options_line = \
    '2023/03/07-16:05:55.538873 8379 [/column_family.cc:631] ' \
    f'--------------- Options for column family [{cf1}]:'


cf1_recovered_line = \
    '2023/03/07-16:05:55.532774 8379 [/version_set.cc:5585] ' \
    f'Column family [{cf1}] (ID {cf1_id}), log number is 0'.strip()


cf1_cf2_id_recovered_line = \
    '2023/03/07-16:05:55.532774 8379 [/version_set.cc:5585] ' \
    f'Column family [{cf1}] (ID {cf2_id}), log number is 0'.strip()

cf1_created_line = \
    '2023/03/07-16:06:09.051479 8432 [/db_impl/db_impl.cc:3200] ' \
    f'Created column family [{cf1}] (ID {cf1_id})'

cf1_cf2_id_created_line = \
    '2023/03/07-16:06:09.051479 8432 [/db_impl/db_impl.cc:3200] ' \
    f'Created column family [{cf1}] (ID {cf2_id})'

cf2_created_line = \
    '2023/03/07-16:06:09.051479 8432 [/db_impl/db_impl.cc:3200] ' \
    f'Created column family [{cf2}] (ID {cf2_id})'

cf2_cf1_id_created_line = \
    '2023/03/07-16:06:09.051479 8432 [/db_impl/db_impl.cc:3200] ' \
    f'Created column family [{cf2}] (ID {cf1_id})'

cf1_drop_time = '2023/03/07-16:06:09.037627'
cf1_dropped_line = \
    f'{cf1_drop_time} 8432 [/db_impl/db_impl.cc:3311] ' \
    f'Dropped column family with id {cf1_id}'

cf2_dropped_line = \
    '2023/03/07-16:06:09.037627 8432 [/db_impl/db_impl.cc:3311] ' \
    f'Dropped column family with id {cf2_id}'

cf1_options_entry = test_utils.line_to_entry(cf1_options_line)
cf1_recovered_entry = test_utils.line_to_entry(cf1_recovered_line)
cf1_cf2_id_recovered_entry = \
    test_utils.line_to_entry(cf1_cf2_id_recovered_line)

cf1_created_entry = test_utils.line_to_entry(cf1_created_line)
cf1_cf2_id_created_entry = test_utils.line_to_entry(cf1_cf2_id_created_line)

cf2_created_entry = test_utils.line_to_entry(cf2_created_line)
cf2_cf1_id_created_entry = test_utils.line_to_entry(cf2_cf1_id_created_line)

cf1_dropped_entry = test_utils.line_to_entry(cf1_dropped_line)
cf2_dropped_entry = test_utils.line_to_entry(cf2_dropped_line)


def test_empty():
    cfs = CfsMetadata("dummy-path")
    assert cfs.get_cf_id(cf1) is None
    assert cfs.get_cf_info_by_name(cf1) is None
    assert cfs.get_cf_info_by_id(cf1_id) is None
    assert cfs.get_non_auto_generated_cfs_names() == []
    assert cfs.get_auto_generated_cf_names() == []
    assert cfs.get_num_cfs() == 0


def test_add_cf_found_during_options_parsing_cf_name_known():
    cfs = CfsMetadata("dummy-path")

    assert cfs.add_cf_found_during_cf_options_parsing(cf1,
                                                      cf1_id,
                                                      is_auto_generated=False,
                                                      entry=cf1_options_entry)
    expected_cf_metadata = \
        CfMetadata(discovery_type=CfDiscoveryType.OPTIONS,
                   name=cf1,
                   discovery_time='2023/03/07-16:05:55.538873',
                   has_options=True,
                   auto_generated=False,
                   id=cf1_id,
                   drop_time=None)
    assert cfs.get_cf_id(cf1) == cf1_id
    assert cfs.get_cf_info_by_name(cf1) == expected_cf_metadata
    assert cfs.get_cf_info_by_id(cf1_id) == expected_cf_metadata
    assert cfs.get_non_auto_generated_cfs_names() == [cf1]
    assert cfs.get_auto_generated_cf_names() == []
    assert cfs.get_num_cfs() == 1
    assert not cfs.was_cf_dropped(cf1)
    assert cfs.get_cf_drop_time(cf1) is None


def test_add_cf_found_during_options_parsing_auto_generated_cf_name():
    cfs = CfsMetadata("dummy-path")

    unknown_cf_name = "Unknonw-1"
    options_entry = \
        test_utils.line_to_entry(
            '2023/01/23-22:09:21.013513 7f6bdfd54700 '
            'Options.comparator: leveldb.BytewiseComparator')

    assert cfs.add_cf_found_during_cf_options_parsing(cf_name=unknown_cf_name,
                                                      cf_id=None,
                                                      is_auto_generated=True,
                                                      entry=options_entry)
    expected_cf_metadata = \
        CfMetadata(discovery_type=CfDiscoveryType.OPTIONS,
                   name=unknown_cf_name,
                   discovery_time='2023/01/23-22:09:21.013513',
                   has_options=True,
                   auto_generated=True,
                   id=None,
                   drop_time=None)
    assert cfs.get_cf_id(cf1) is None
    assert cfs.get_cf_info_by_name(unknown_cf_name) == expected_cf_metadata
    assert cfs.get_non_auto_generated_cfs_names() == []
    assert cfs.get_auto_generated_cf_names() == [unknown_cf_name]
    assert cfs.get_num_cfs() == 1
    assert not cfs.was_cf_dropped(cf1)
    assert cfs.get_cf_drop_time(cf1) is None


def test_handle_cf_name_found_during_parsing():
    cfs = CfsMetadata("dummy-path")

    assert cfs.add_cf_found_during_cf_options_parsing(cf1,
                                                      cf1_id,
                                                      is_auto_generated=False,
                                                      entry=cf1_options_entry)

    stats_start_entry =\
        test_utils.line_to_entry('2023/01/23-22:19:21.014278 7f6bdfd54700 ['
                                 '/db_impl/db_impl.cc:947]')
    assert not cfs.handle_cf_name_found_during_parsing(cf1, stats_start_entry)
    assert cfs.get_num_cfs() == 1

    stats_start_entry =\
        test_utils.line_to_entry('2023/01/23-22:19:21.014278 7f6bdfd54700 ['
                                 '/db_impl/db_impl.cc:947]')
    assert cfs.handle_cf_name_found_during_parsing(cf2,
                                                   stats_start_entry)
    expected_cf_metadata = \
        CfMetadata(discovery_type=CfDiscoveryType.DURING_PARSING,
                   name=cf2,
                   discovery_time='2023/01/23-22:19:21.014278',
                   has_options=False,
                   auto_generated=False,
                   id=None,
                   drop_time=None)
    assert cfs.get_cf_id(cf2) is None
    assert cfs.get_cf_info_by_name(cf2) == expected_cf_metadata
    assert cfs.get_cf_info_by_id(cf2_id) is None
    assert cfs.get_non_auto_generated_cfs_names() == [cf1, cf2]
    assert cfs.get_auto_generated_cf_names() == []
    assert cfs.get_num_cfs() == 2


def test_parse_cf_recovered_entry():
    cfs = CfsMetadata("dummy-path")

    assert cfs.add_cf_found_during_cf_options_parsing(cf1,
                                                      cf_id=None,
                                                      is_auto_generated=False,
                                                      entry=cf1_options_entry)
    assert cfs.get_cf_id(cf1) is None

    assert cfs.try_parse_as_cf_lifetime_entries(
        [cf1_recovered_entry], entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == 10

    # Illegal (different cf_id), but should be silently ignored
    assert cfs.try_parse_as_cf_lifetime_entries(
        [cf1_cf2_id_recovered_entry], entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == 10

    assert cfs.try_parse_as_cf_lifetime_entries([cf1_recovered_entry],
                                                entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == 10


def test_parse_cf_recovered_entry_2():
    cfs = CfsMetadata("dummy-path")

    # Recovered without options
    assert cfs.try_parse_as_cf_lifetime_entries(
        [cf1_recovered_entry], entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == 10

    # Already discovered without options => Rejected
    assert not cfs.add_cf_found_during_cf_options_parsing(
        cf1, cf_id=None, is_auto_generated=False, entry=cf1_recovered_entry)
    assert cfs.get_cf_id(cf1) == 10


def test_parse_cf_created_entry():
    cfs = CfsMetadata("dummy-path")

    assert cfs.add_cf_found_during_cf_options_parsing(
        cf1, cf_id=None, is_auto_generated=False, entry=cf1_recovered_entry)
    assert cfs.get_cf_id(cf1) is None

    assert cfs.try_parse_as_cf_lifetime_entries([cf1_created_entry],
                                                entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == cf1_id

    # Already discovered without options => Rejected
    assert cfs.try_parse_as_cf_lifetime_entries([cf1_cf2_id_recovered_entry],
                                                entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == cf1_id


def test_parse_cf_created_entry_2():
    cfs = CfsMetadata("dummy-path")

    # Created without options
    cfs.try_parse_as_cf_lifetime_entries(
        [cf1_created_entry], entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == cf1_id

    assert not cfs.add_cf_found_during_cf_options_parsing(
        cf1, cf_id=None, is_auto_generated=False, entry=cf1_recovered_entry)
    assert cfs.get_cf_id(cf1) == cf1_id


def test_parse_cf_dropped_entry():
    cfs = CfsMetadata("dummy-path")

    # Dropping a cf doesn't specify the dropped cf's name, only its id =>
    # We may have an auto-generated cf (=> no id) that is later dropped
    cfs.try_parse_as_cf_lifetime_entries([cf1_dropped_entry],
                                         entry_idx=0)
    assert not cfs.was_cf_dropped(cf1)
    assert cfs.get_cf_drop_time(cf1) is None

    assert cfs.add_cf_found_during_cf_options_parsing(
        cf1, cf_id=cf1_id, is_auto_generated=False, entry=cf1_recovered_entry)
    assert cfs.get_cf_id(cf1) == cf1_id

    assert cfs.try_parse_as_cf_lifetime_entries([cf1_dropped_entry],
                                                entry_idx=0) == (True, 1)
    assert cfs.get_cf_id(cf1) == cf1_id
    assert cfs.was_cf_dropped(cf1)
    assert cfs.get_cf_drop_time(cf1) == cf1_drop_time

    assert cfs.try_parse_as_cf_lifetime_entries(
        [cf1_dropped_entry], entry_idx=0) == (True, 1)
    assert cfs.was_cf_dropped(cf1)
    assert cfs.get_cf_drop_time(cf1) == cf1_drop_time
