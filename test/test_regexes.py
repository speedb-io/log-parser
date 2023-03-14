import re

import regexes as r


def test_cf_name_regex():
    assert re.search(r.CF_NAME, "[CF1]").group('cf') == 'CF1'
    assert re.search(r.CF_NAME, "[ C F 1 ]").group('cf') == ' C F 1 '
    assert re.search(r.CF_NAME, "[]").group('cf') == ''
    assert re.search(r.CF_NAME, "[...]").group('cf') == '...'
    assert re.search(r.CF_NAME, "[\0\1]").group('cf') == '\0\1'
    assert re.search(r.CF_NAME, "[!@#$%_-=]").group('cf') == '!@#$%_-='
    assert re.search(r.CF_NAME, "CF1") is None
    assert re.search(r.CF_NAME, "CF1]") is None
    assert re.search(r.CF_NAME, "[CF1") is None


def test_uptime_stats_line_regex():
    line1 = "Uptime(secs): 654.3 total, 789.1 interval"
    line2 = " Uptime(secs):123.4 total, 56.7 interval  "
    line3 = "Uptime(secs):123.4 total,"
    line4 = "Uptime(secs):123.4 total, 78.9"

    m1 = re.search(r.UPTIME_STATS_LINE, line1)
    assert m1
    assert m1.group('total') == '654.3'
    assert m1.group('interval') == '789.1'

    m2 = re.search(r.UPTIME_STATS_LINE, line2)
    assert m2
    assert m2.group('total') == '123.4'
    assert m2.group('interval') == '56.7'

    assert not re.search(r.UPTIME_STATS_LINE, line3)
    assert not re.search(r.UPTIME_STATS_LINE, line4)


def test_db_wide_interval_stall_regex():
    line = 'Interval stall: 01:02:3.456 H:M:S, 7.8 percent'
    m = re.search(r.DB_WIDE_INTERVAL_STALL, line)
    assert m
    assert m.groups() == ('01', '02', '3', '456', '7.8')


def test_db_wide_cumulative_stall_regex():
    line = 'Cumulative stall: 01:02:3.456 H:M:S, 7.8 percent'
    m = re.search(r.DB_WIDE_CUMULATIVE_STALL, line)
    assert m
    assert m.groups() == ('01', '02', '3', '456', '7.8')


def test_db_wide_cumulative_writes():
    line = 'Cumulative writes: 819K writes, 1821M keys, 788K commit groups, ' \
           '1.0 writes per commit group, ingest: 80.67 GB, 68.66 MB/s '
    m = re.search(r.DB_WIDE_CUMULATIVE_WRITES, line)
    assert m
    assert m.groups() == ('819', 'K', '1821', 'M', '80.67', '68.66')
