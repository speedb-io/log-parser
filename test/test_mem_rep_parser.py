import mem_rep_parser
import test.testing_utils as test_utils

MRP = mem_rep_parser.MemRepParser


def test_is_start_line():
    assert MRP.is_start_line("** Memory Reporting **")

    assert not MRP.is_start_line("")
    assert not MRP.is_start_line("Memory Reporting")
    assert not MRP.is_start_line("**    Memory     Reporting **")


def test_is_total_line():
    assert MRP.is_total_line("Total: 100")

    assert not MRP.is_total_line("Total:100")
    assert not MRP.is_total_line("Total:")


def test_is_arena_stats_title_line():
    assert MRP.is_arena_stats_title_line("Arena Stats:")

    assert not MRP.is_arena_stats_title_line("Arena Stats")
    assert not MRP.is_arena_stats_title_line("Arena  Stats:")


def test_parse_arena_stats_lines():
    lines1 = \
    '''Arena Stats:
    Total: 16M
    ArenaWrappedDBIter: 0''' # noqa
    lines1 = [line.strip() for line in lines1.splitlines()]
    expected1 = ("16M", {"ArenaWrappedDBIter": "0"}, 3)

    lines2 = \
    '''Arena Stats:
    Total: 26M
    ArenaWrappedDBIter: 0
    HashSpdb: 26M
    CF Stats:'''.splitlines() # noqa
    lines2 = [line.strip() for line in lines2]
    expected2 = ("26M", {"ArenaWrappedDBIter": "0",
                         "HashSpdb": "26M"}, 4)

    lines3 = \
    '''Arena Stats:
    Total: 26M
    ArenaWrappedDBIter: 0
    HashSpdb: 26M    
    UNRELATED LINE'''.splitlines() # noqa
    lines3 = [line.strip() for line in lines2]
    expected3 = expected2

    lines_list = [lines1, lines2, lines3]
    expected = [expected1, expected2, expected3]

    for i, lines in enumerate(lines_list):
        assert MRP.parse_arena_stats_lines(lines_list[i], 0) == expected[i]


def test_parse_cfs_stats_lines():
    lines1 = \
    '''CF Stats: 
    Total: 16M
    [default]: 16M
    '''  # noqa
    lines1 = [line.strip() for line in lines1.splitlines()]
    expected1 = ("16M", {"default": "16M"}, 3)

    lines2 = \
    '''CF Stats: 
    Total: 26M
    [default]: 26M
    rocksdb.block-cache-usage: 96
    '''  # noqa
    lines2 = [line.strip() for line in lines2.splitlines()]
    expected2 = ("26M", {"default": "26M"}, 3)

    lines3 = \
    '''CF Stats: 
    Total: 36M
    [default]: 36M
    [cf1]: 1000
    [cf2]: 2000
    rocksdb.block-cache-usage: 96
    '''  # noqa
    lines3 = [line.strip() for line in lines3.splitlines()]
    expected3 = ("36M", {"default": "36M",
                         "cf1": "1000",
                         "cf2": "2000"}, 5)

    lines_list = [lines1, lines2, lines3]
    expected = [expected1, expected2, expected3]

    for i, lines in enumerate(lines_list):
        assert MRP.parse_cfs_stats_lines(lines_list[i], 0) == expected[i]


def test_parse_misc_stats_lines():
    lines1 = \
    '''rocksdb.f1: 96
    Total CacheAllocationUsage: 655K
    2024/01/07-15:05:10.252242 55366 NEXT LINE
    '''  # noqa
    lines1 = [line.strip() for line in lines1.splitlines()]
    expected1 = ({"rocksdb.f1": "96",
                  "Total CacheAllocationUsage": "655K"}, 2)

    lines_list = [lines1]
    expected = [expected1]

    for i, lines in enumerate(lines_list):
        assert MRP.parse_misc_stats_lines(lines_list[i], 0) == expected[i]


def test_add_lines():
    lines = '''** Memory Reporting **
    Arena Stats:
    Total: 16M
    ArenaWrappedDBIter: 0
    HashSpdb: 16M
    CF Stats:
    Total: 26M
    [default]: 26M
    [cf1]: 1000
    rocksdb.block-cache-usage: 96
    Total CacheAllocationUsage: 655K
    2024/01/07-15:05:10.252242 55366 [/db_impl/db_impl_write.cc:2197] XXXX
    '''
    lines = [line.strip() for line in lines.splitlines()]
    expected_arena_stats = {"ArenaWrappedDBIter": "0",
                            "HashSpdb": "16M"}
    expected_cfs_stats = {"default": "26M",
                          "cf1": "1000"}
    expected_misc_stats = {"rocksdb.block-cache-usage": "96",
                           "Total CacheAllocationUsage": "655K"}
    expected_report = MRP.Report("16M",
                                 expected_arena_stats,
                                 "26M",
                                 expected_cfs_stats,
                                 expected_misc_stats)

    time = "2024/01/07-15:05:09.261322"
    expected_reports = {time: expected_report}

    mrp = MRP()
    mrp.add_lines(time, lines)

    assert mrp.get_reports() == expected_reports


def test_try_adding_entries():
    time = "2024/01/07-15:05:09.261322"
    lines = f'''{time} 55365 [/db_impl/db_impl.cc:1196]
    ** Memory Reporting **
    Arena Stats:
    Total: 16M
    ArenaWrappedDBIter: 0
    HashSpdb: 16M
    CF Stats:
    Total: 26M
    [default]: 26M
    [cf1]: 1000
    rocksdb.block-cache-usage: 96
    Total CacheAllocationUsage: 655K'''.splitlines()

    entry = test_utils.lines_to_entry(lines)
    expected_arena_stats = {"ArenaWrappedDBIter": "0",
                            "HashSpdb": "16M"}
    expected_cfs_stats = {"default": "26M",
                          "cf1": "1000"}
    expected_misc_stats = {"rocksdb.block-cache-usage": "96",
                           "Total CacheAllocationUsage": "655K"}
    expected_report = MRP.Report("16M",
                                 expected_arena_stats,
                                 "26M",
                                 expected_cfs_stats,
                                 expected_misc_stats)
    expected_reports = {time: expected_report}

    mrp = MRP()

    assert mrp.try_adding_entries([entry], 0) == (True, 1)
    assert mrp.get_reports() == expected_reports

    # Omit header line
    invalid_lines = [line for i, line in enumerate(lines) if i != 1]
    invalid_entry = test_utils.lines_to_entry(invalid_lines)
    assert mrp.try_adding_entries([invalid_entry], 0) == (False, 0)
