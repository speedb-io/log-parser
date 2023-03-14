from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass
from enum import auto

import events
import regexes
import utils


class CompactionState:
    WAITING_START = auto()
    STARTED = auto()
    FINISHED = auto()


@dataclass
class PreFinishStatsInfo:
    cf_name: str
    read_rate_mbps: float = 0.0
    write_rate_mbps: float = 0.0
    read_write_amplify: float = 0.0
    write_amplify: float = 0.0
    records_in: int = 0
    records_dropped: int = 0

    def as_dict(self):
        return asdict(self)


@dataclass
class CompactionJobInfo:
    job_id: int
    cf_name: str = None
    start_event: events.CompactionStartedEvent = None
    finish_event: events.CompactionFinishedEvent = None
    pre_finish_info: PreFinishStatsInfo = None

    def __str__(self):
        return f"Compaction Info: " \
               f"job_id:{self.job_id}, " \
               f"cf_name:{self.cf_name}" \
               f"start_time:{self.get_start_time()}" \
               f"finish_time:{self.get_finish_time()}"

    def get_state(self):
        if self.finish_event is not None:
            assert self.start_event is not None
            return CompactionState.FINISHED
        elif self.start_event is not None:
            return CompactionState.STARTED
        else:
            return CompactionState.WAITING_START

    def has_finished(self):
        return self.get_state() == CompactionState.FINISHED

    def get_start_time(self):
        return self.start_event.get_log_time() if self.start_event else ""

    def get_finish_time(self):
        return self.finish_event.get_log_time() if self.finish_event else ""

    def set_start_event(self, start_event):
        if self.get_state() != CompactionState.WAITING_START:
            raise utils.ParsingError(f"Unexpected Compaction's state to "
                                     f"set start event ({start_event}. {self}")
        self.start_event = start_event

    def set_finish_event(self, finish_event):
        if self.get_state() != CompactionState.STARTED:
            raise utils.ParsingError(f"Unexpected Compaction's state to set "
                                     f"finish event ({finish_event}. {self}")
        if utils.compare_times_strs(self.get_start_time(),
                                    finish_event.get_log_time()) > 0:
            raise utils.ParsingError(
                f"finish event's time ({finish_event} before "
                f"start event time.\n{finish_event}")
        self.finish_event = finish_event


class CompactionsMonitor:
    def __init__(self):
        self.jobs = dict()
        self.pre_finish_stats_infos = list()

    def consider_entry(self, entry):
        try:
            is_finish_stats_line, cf_name = \
                self.try_parse_as_pre_finish_stats_line(entry)
            if is_finish_stats_line:
                return True, cf_name
        except utils.ParsingError as e:
            logging.WARN(f"Error adding entry to compaction jobs.\n"
                         f"error: {e}\n"
                         f"entry:{entry}")

        return False, None

    def add_job_if_applicable(self, job_id, cf_name=None, entry=None):
        entry_cf_name = entry.get_cf_name() if entry else cf_name
        entry_job_id = entry.get_job_id() if entry else job_id

        if cf_name is not None and entry_cf_name != cf_name or \
                entry_job_id != job_id:
            raise utils.ParsingError(
                f"entry's cf name ({entry_cf_name}) or entry_job_id "
                f"({entry_job_id}) != what was parsed here.\nentry: {entry}")

        if job_id not in self.jobs:
            self.jobs[job_id] = CompactionJobInfo(job_id, cf_name)
        else:
            if self.jobs[job_id].cf_name != cf_name:
                raise utils.ParsingError(
                    f"new cf_name ({cf_name}) != existing ("
                    f"{self.jobs[job_id].cf_name}) for same job.")

    def try_parse_as_score_entry(self, entry):
        match = \
            re.findall(regexes.COMPACTION_BEFORE_SCORE_LINE, entry.get_msg())
        if not match:
            return False
        assert len(match) == 1 and len(match[0]) == 4

        cf_name, job_id, _, before_score = match[0]
        job_id = int(job_id)
        before_score = float(before_score)

        self.add_job_if_applicable(job_id, cf_name, entry)
        self.jobs[job_id].before_score = before_score

        return True

    def try_parse_as_pre_finish_stats_line(self, entry):
        match = re.findall(regexes.COMPACTION_JOB_FINISH_STATS_LINE,
                           entry.get_msg())
        if not match:
            return False, None
        assert len(match) == 1 and len(match[0]) == 7

        info = PreFinishStatsInfo(*match[0])
        info.read_rate_mbps = float(info.read_rate_mbps)
        info.write_rate_mbps = float(info.write_rate_mbps)
        info.read_write_amplify = float(info.read_write_amplify)
        info.write_amplify = float(info.write_amplify)
        info.records_in = int(info.records_in)
        info.records_dropped = int(info.records_dropped)

        self.pre_finish_stats_infos.append(info)

        return True, info.cf_name

    def new_event(self, event):
        event_type = event.get_type()
        if event_type == events.EventType.COMPACTION_STARTED:
            self.compaction_started(event)
        elif event_type == events.EventType.COMPACTION_FINISHED:
            self.compaction_finished(event)

    def compaction_started(self, event):
        assert isinstance(event, events.CompactionStartedEvent)

        # 2023/01/04-08:55:00.743718 27420 EVENT_LOG_v1
        # {"time_micros": 1672822500743711, "job": 9,
        # "event": "compaction_started",
        # "compaction_reason": "LevelL0FilesNum",
        # "files_L0": [17250, 17247, 17243, 17239], "score": 1,
        # "input_data_size": 251316602}
        #
        job_id = event.get_job_id()
        self.add_job_if_applicable(job_id, event.get_cf_name())
        self.jobs[job_id].set_start_event(event)

    def compaction_finished(self, event):
        # 2023/01/04-08:55:00.746783 27413
        # (Original Log Time 2023/01/04-08:55:00.746653) EVENT_LOG_v1
        # {"time_micros": 1672822500746645, "job": 4,
        # "event": "compaction_finished",
        # "compaction_time_micros": 971568,
        # "compaction_time_cpu_micros": 935180,
        # "output_level": 1, "num_output_files": 7,
        # "total_output_size": 437263613,
        # "num_input_records": 424286, "num_output_records": 423497,
        # "num_subcompactions": 1, "output_compression": "NoCompression",
        # "num_single_delete_mismatches": 0,
        # "num_single_delete_fallthrough": 0,
        # "lsm_state": [4, 7, 45, 427, 822, 0, 0]}
        #
        assert isinstance(event, events.CompactionFinishedEvent)

        job_id = event.get_job_id()
        if job_id not in self.jobs:
            logging.info(
                f"Compaction finished event for job for which there is no "
                f"recorded compaction started. job-id:{job_id}\n"
                f"event:{event}")
            return

        job = self.jobs[job_id]
        job.set_finish_event(event)

        # Try to match the pre-finish info based on the number of
        # input records and cf name
        num_input_records = event.get_num_input_records()
        for idx, info in enumerate(self.pre_finish_stats_infos):
            if num_input_records == info.records_in:
                if job.cf_name == info.cf_name:
                    job.pre_finish_info = info
                    self.pre_finish_stats_infos.pop(idx)
                    break
                else:
                    logging.info(
                        f"# input records match, but different cf-s.\n"
                        f"pre-finish info:{info}\n"
                        f"job:{asdict(job)}\n"
                        f"event:{event}")

    def get_finished_jobs(self):
        finished_jobs = dict()
        for job_id, job_info in self.jobs.items():
            if job_info.has_finished():
                finished_jobs[job_id] = job_info
        return finished_jobs

    def get_cf_finished_jobs(self, cf_name):
        finished_cf_jobs = dict()
        for job_id, job_info in self.jobs.items():
            if job_info.has_finished() and job_info.cf_name == cf_name:
                finished_cf_jobs[job_id] = job_info
        return finished_cf_jobs
