import logging
from dataclasses import dataclass

import db_files
from counters import CountersMngr
from db_files import DbFilesMonitor
from db_options import RAW_NULL_PTR, DatabaseOptions, \
    sanitized_to_raw_ptr_value


def get_cf_raw_cache_ptr_str(cf_name, db_options):
    assert isinstance(db_options, DatabaseOptions)

    sanitized_cache_ptr = \
        db_options.get_cf_table_option(cf_name, "block_cache")
    return sanitized_to_raw_ptr_value(sanitized_cache_ptr)


def does_cf_has_cache(cf_name, db_options):
    raw_ptr_str = db_options.get_cf_table_raw_ptr_str(cf_name, "block_cache")
    return raw_ptr_str is not None and raw_ptr_str != RAW_NULL_PTR


def get_cache_id(cf_name, db_options):
    assert isinstance(db_options, DatabaseOptions)

    cache_ptr = db_options.get_cf_table_raw_ptr_str(cf_name, "block_cache")
    cache_name = db_options.get_cf_table_option(cf_name, "block_cache_name")

    if cache_ptr is None or cache_name is None:
        return None

    return f"{cache_name}@{cache_ptr}"


@dataclass
class CfCacheOptions:
    cf_name: str
    cache_id: str = None
    cache_index_and_filter_blocks: bool = None
    cache_capacity_bytes: int = 0
    num_shard_bits: int = 0


@dataclass
class CfSpecificCacheOptions:
    cache_index_and_filter_blocks: bool = None


@dataclass
class CacheOptions:
    cache_capacity_bytes: int = 0
    num_shard_bits: int = 0
    shard_size_bytes: int = 0
    # {<cf-name>: CfSpecificCacheOptions}
    cfs_specific_options: dict = None


def collect_cf_cache_options(cf_name, db_options):
    assert isinstance(db_options, DatabaseOptions)

    if not does_cf_has_cache(cf_name, db_options):
        return None

    cache_id = get_cache_id(cf_name, db_options)
    cache_index_and_filter_blocks = \
        db_options.get_cf_table_option(
            cf_name, "cache_index_and_filter_blocks")
    cache_capacity_bytes_str = \
        db_options.get_cf_table_option(cf_name, "block_cache_capacity")
    num_shard_bits_str = \
        db_options.get_cf_table_option(cf_name, "block_cache_num_shard_bits")

    if cache_id is None or cache_index_and_filter_blocks is None or \
            cache_capacity_bytes_str is None or num_shard_bits_str is None:
        logging.warning(
            f"{cf_name} has cache but its cache options are "
            f"corrupted. cache_id:{cache_id}, "
            f"cache_index_and_filter_blocks:{cache_index_and_filter_blocks}"
            f"cache_capacity_bytes_str:{cache_capacity_bytes_str}"
            f"num_shard_bits_str:{num_shard_bits_str}")
        return None

    options = CfCacheOptions(cf_name)
    options.cache_id = cache_id
    options.cache_index_and_filter_blocks = cache_index_and_filter_blocks
    options.cache_capacity_bytes = int(cache_capacity_bytes_str)
    options.num_shard_bits = int(num_shard_bits_str)

    return options


def calc_shard_size_bytes(cache_capacity_bytes, num_shard_bits):
    num_shards = 2 ** num_shard_bits
    return int((cache_capacity_bytes + (num_shards - 1)) / num_shards)


def collect_cache_options_info(db_options):
    assert isinstance(db_options, DatabaseOptions)

    # returns {<cache-id>: [<cf-cache-options>]
    cache_options = dict()

    cfs_names = db_options.get_cfs_names()
    for cf_name in cfs_names:
        cf_cache_options = collect_cf_cache_options(cf_name, db_options)
        if cf_cache_options is None:
            continue

        cache_id = cf_cache_options.cache_id
        cfs_specific_options =\
            CfSpecificCacheOptions(
                cf_cache_options.cache_index_and_filter_blocks)

        if cache_id not in cache_options:
            shard_size_bytes = \
                calc_shard_size_bytes(cf_cache_options.cache_capacity_bytes,
                                      cf_cache_options.num_shard_bits)

            cfs_specific_options =\
                {cf_cache_options.cf_name: cfs_specific_options}
            cache_options[cache_id] = \
                CacheOptions(
                    cache_capacity_bytes=cf_cache_options.cache_capacity_bytes,
                    num_shard_bits=cf_cache_options.num_shard_bits,
                    shard_size_bytes=shard_size_bytes,
                    cfs_specific_options=cfs_specific_options)
        else:
            assert cache_options[cache_id].cache_capacity_bytes == \
                   cf_cache_options.cache_capacity_bytes
            assert cache_options[cache_id].num_shard_bits == \
                   cf_cache_options.num_shard_bits
            cache_options[cache_id].cfs_specific_options[
                cf_cache_options.cf_name] = cfs_specific_options

    if not cache_options:
        return None

    return cache_options


@dataclass
class CacheCounters:
    cache_add: int = 0
    cache_miss: int = 0
    cache_hit: int = 0
    index_add: int = 0
    index_miss: int = 0
    index_hit: int = 0
    filter_add: int = 0
    filter_miss: int = 0
    filter_hit: int = 0
    data_add: int = 0
    data_miss: int = 0
    data_hit: int = 0


def collect_cache_counters(counters_mngr):
    assert isinstance(counters_mngr, CountersMngr)

    if not counters_mngr.does_have_counters_values():
        logging.info("Can't collect cache counters. No counters available")
        return None

    cache_counters_names = {
        "cache_miss": "rocksdb.block.cache.miss",
        "cache_hit": "rocksdb.block.cache.hit",
        "cache_add": "rocksdb.block.cache.add",
        "index_miss": "rocksdb.block.cache.index.miss",
        "index_hit": "rocksdb.block.cache.index.hit",
        "index_add": "rocksdb.block.cache.index.add",
        "filter_miss": "rocksdb.block.cache.filter.miss",
        "filter_hit": "rocksdb.block.cache.filter.hit",
        "filter_add": "rocksdb.block.cache.filter.add",
        "data_miss": "rocksdb.block.cache.data.miss",
        "data_hit": "rocksdb.block.cache.data.hit",
        "data_add": "rocksdb.block.cache.data.add"}

    counters = CacheCounters()

    for field_name, counter_name in cache_counters_names.items():
        counter_value = counters_mngr.get_last_counter_value(counter_name)
        setattr(counters, field_name, counter_value)

    return counters


@dataclass
class CacheIdInfo:
    options: CacheOptions = None
    files_stats: db_files.CfsFilesStats = None


@dataclass
class CacheStats:
    # {<cache-id>: CacheIdInfo}
    per_cache_id_info: dict = None
    global_cache_counters: CacheCounters = None


def calc_block_cache_stats(db_options: object, counters_mngr,
                           files_monitor) -> object:
    assert isinstance(db_options, DatabaseOptions)
    assert isinstance(counters_mngr, CountersMngr)
    assert isinstance(files_monitor, DbFilesMonitor)

    stats = CacheStats()

    cache_options = collect_cache_options_info(db_options)
    if cache_options is None:
        return None

    stats.per_cache_id_info = dict()
    for cache_id, options in cache_options.items():
        assert isinstance(options, CacheOptions)

        cache_cfs_names = list(options.cfs_specific_options.keys())
        cache_files_stats =\
            db_files.calc_cf_files_stats(cache_cfs_names, files_monitor)
        if not cache_files_stats:
            return None

        assert isinstance(cache_files_stats, db_files.CfsFilesStats)

        stats.per_cache_id_info[cache_id] = \
            CacheIdInfo(options=options, files_stats=cache_files_stats)

    cache_counters = collect_cache_counters(counters_mngr)
    if cache_counters:
        stats.global_cache_counters = cache_counters

    return stats
