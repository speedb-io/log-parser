import pytest

import compactions
import utils
from events import EventType
from test.testing_utils import create_event, line_to_entry

job_id1 = 1
job_id2 = 2

cf1 = "cf1"
cf2 = "cf2"
cf_names = [cf1, cf2]

time1_minus_10_sec = "2023/01/24-08:54:30.130553"
time1 = "2023/01/24-08:54:40.130553"
time1_plus_10_sec = "2023/01/24-08:54:50.130553"
time1_plus_11_sec = "2023/01/24-08:55:50.130553"

time2 = "2023/01/24-08:55:40.130553"
time2_plus_5_sec = "2023/01/24-08:55:45.130553"

reason1 = "Reason1"
reason2 = "Reason2"

files_l1 = [17248]
files_l2 = [16778, 16779, 16780, 16781, 17022]
input_files = {
    1: files_l1,
    2: files_l2
}


def test_compaction_info():
    info = compactions.CompactionJobInfo(job_id1)
    assert info.get_state() == compactions.CompactionState.WAITING_START

    start_event1 = create_event(job_id1, cf_names, time1_minus_10_sec,
                                EventType.COMPACTION_STARTED, cf1,
                                compaction_reason=reason1)
    start_event2 = create_event(job_id1, cf_names, time1,
                                EventType.COMPACTION_STARTED, cf1,
                                compaction_reason=reason1)

    finish_event1 = create_event(job_id1, cf_names, time1_minus_10_sec,
                                 EventType.COMPACTION_FINISHED, cf1)
    finish_event2 = create_event(job_id1, cf_names, time1_plus_10_sec,
                                 EventType.COMPACTION_FINISHED, cf1)
    finish_event3 = create_event(job_id1, cf_names, time1_plus_11_sec,
                                 EventType.COMPACTION_FINISHED, cf1)

    with pytest.raises(utils.ParsingError):
        info.set_finish_event(finish_event1)

    info.set_start_event(start_event2)
    assert info.get_state() == compactions.CompactionState.STARTED
    with pytest.raises(utils.ParsingError):
        info.set_start_event(start_event1)

    with pytest.raises(utils.ParsingError):
        info.set_finish_event(finish_event1)

    info.set_finish_event(finish_event2)
    assert info.get_state() == compactions.CompactionState.FINISHED

    with pytest.raises(utils.ParsingError):
        info.set_finish_event(finish_event3)


def test_mon_basic():
    monitor = compactions.CompactionsMonitor()
    assert monitor.get_finished_jobs() == {}

    # Compaction Job 1 - Start + Finish Events
    start_event1 = create_event(job_id1, cf_names, time1,
                                EventType.COMPACTION_STARTED, cf1,
                                compaction_reason=reason1,
                                files_L1=files_l1, files_L2=files_l2)
    monitor.new_event(start_event1)
    assert monitor.get_finished_jobs() == {}

    finish_event1 = create_event(job_id1, cf_names, time1_plus_10_sec,
                                 EventType.COMPACTION_FINISHED, cf1)
    monitor.new_event(finish_event1)

    expected_jobs = {
        job_id1: compactions.CompactionJobInfo(job_id1,
                                               cf_name=cf1,
                                               start_event=start_event1,
                                               finish_event=finish_event1,
                                               pre_finish_info=None)
    }
    assert monitor.get_finished_jobs() == expected_jobs

    # Compaction Job 2 - Start + Finish Events
    start_event2 = create_event(job_id2, cf_names, time2,
                                EventType.COMPACTION_STARTED, cf2,
                                compaction_reason=reason2)
    monitor.new_event(start_event2)
    assert monitor.get_finished_jobs() == expected_jobs

    finish_event2 = create_event(job_id2, cf_names, time2_plus_5_sec,
                                 EventType.COMPACTION_FINISHED, cf2)
    monitor.new_event(finish_event2)

    expected_jobs[job_id2] = \
        compactions.CompactionJobInfo(job_id2,
                                      cf_name=cf2,
                                      start_event=start_event2,
                                      finish_event=finish_event2)
    assert monitor.get_finished_jobs() == expected_jobs

    expected_cf1_jobs = {
        job_id1: compactions.CompactionJobInfo(job_id1,
                                               cf_name=cf1,
                                               start_event=start_event1,
                                               finish_event=finish_event1,
                                               pre_finish_info=None)
    }
    assert monitor.get_cf_finished_jobs(cf1) == expected_cf1_jobs

    expected_cf2_jobs = {
        job_id2: compactions.CompactionJobInfo(job_id2,
                                               cf_name=cf2,
                                               start_event=start_event2,
                                               finish_event=finish_event2,
                                               pre_finish_info=None)
    }
    assert monitor.get_cf_finished_jobs(cf2) == expected_cf2_jobs


def test_try_parse_as_pre_finish_stats_line():
    max_score = 4.56
    read_rate_mbps = 475.6
    write_rate_mbps = 475.0
    level = 10
    read_write_amplify = 1.2
    write_amplify = 3.4
    records_in = 5678
    records_dropped = 9876

    pre_fihish_entry = \
        line_to_entry(
            f"{time1} 1234 (Original Log Time {time1_minus_10_sec}) "
            f"[/compaction/compaction_job.cc:952] "
            f"[{cf2}] compacted to: files[3 7 45 427 822 0 0] "
            f"max score {max_score}, "
            f"MB/sec: {read_rate_mbps} rd, {write_rate_mbps} wr, "
            f"level {level}, files in(0, 4) out(1 +0 blob) "
            f"MB in(0.0, 239.7 +0.0 blob) out(239.4 +0.0 blob), "
            f"read-write-amplify({read_write_amplify}) "
            f"write-amplify({write_amplify}) OK, "
            f"records in: {records_in}, records dropped: {records_dropped} "
            f"output_compression: NoCompression")

    start_event = create_event(job_id1, cf_names, time1,
                               EventType.COMPACTION_STARTED, cf2,
                               compaction_reason=reason1,
                               files_L1=files_l1, files_L2=files_l2)

    finish_event = create_event(job_id1, cf_names, time1_plus_11_sec,
                                EventType.COMPACTION_FINISHED, cf2,
                                num_input_records=records_in)

    monitor = compactions.CompactionsMonitor()
    monitor.new_event(start_event)
    assert monitor.consider_entry(pre_fihish_entry) == (True, cf2)
    monitor.new_event(finish_event)

    expected_pre_finish_info = \
        compactions.PreFinishStatsInfo(cf_name=cf2,
                                       read_rate_mbps=read_rate_mbps,
                                       write_rate_mbps=write_rate_mbps,
                                       read_write_amplify=read_write_amplify,
                                       write_amplify=write_amplify,
                                       records_in=records_in,
                                       records_dropped=records_dropped)

    expected_jobs = {
        job_id1: compactions.CompactionJobInfo(
            job_id=job_id1,
            cf_name=cf2,
            start_event=start_event,
            finish_event=finish_event,
            pre_finish_info=expected_pre_finish_info)
    }
    assert monitor.get_finished_jobs() == expected_jobs
