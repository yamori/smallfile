#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import smallfile
from smallfile import SMFResultException, KB_PER_GB
import json

def output_results(invoke_list, prm):
    if prm.output_json:
        output_results_in_json(invoke_list, prm)
    else:
        output_results_to_human(invoke_list, prm)

# construct a hierarchy of dictionaries that represents smallfile output
# and let JSON module dump it

def output_results_in_json(invoke_list, prm):
    json_obj = {}
    json_obj['hosts'] = {}
    json_obj['files'] = 0
    json_obj['records'] = 0
    json_obj['elapsed'] = 0.0
    json_obj['warnings'] = {}  # key is warning, value is True

    for i in invoke_list:

        # create per-host entry which will contain per-thread data
        # plus aggregation of thread data on this host

        try:
            on_host_d = json_obj['hosts'][i.onhost]
        except KeyError:
            json_obj['hosts'][i.onhost] = {}
            on_host_d = json_obj['hosts'][i.onhost]
            on_host_d['threads'] = {}
            on_host_d['files'] = 0
            on_host_d['records'] = 0
            on_host_d['elapsed'] = 0.0
        try:
            thread_set = on_host_d['threads']
        except KeyError:
            on_host_d['threads'] = {}
            thread_set = on_host_d['threads']
        #print(json_obj)

        # construct dictionary of results for this thread
        # insert this thread's results into the JSON thread hierarchy
        # and aggregate totals for the host

        invobj = {}
        status = 'ok'
        if i.status:
            status = os.strerror(i.status)
            json_obj['warnings']['thread-failures'] = True
        invobj['status'] = status
        invobj['elapsed'] = i.elapsed_time
        invobj['files'] = i.filenum_final
        invobj['records'] = i.rq_final
        thread_set[i.tid] = invobj
        on_host_d['files'] += i.filenum_final
        on_host_d['records'] += i.rq_final
        on_host_d['elapsed'] = max(on_host_d['elapsed'], i.elapsed_time)
        json_obj['files'] += i.filenum_final
        json_obj['records'] += i.rq_final
        json_obj['elapsed'] = max(on_host_d['elapsed'], i.elapsed_time)

    # compute aggregate results

    my_host_invoke = prm.master_invoke
    rszkb = my_host_invoke.record_sz_kb
    if rszkb == 0:
        rszkb = my_host_invoke.total_sz_kb
    if rszkb * my_host_invoke.BYTES_PER_KB > my_host_invoke.biggest_buf_size:
        rszkb = my_host_invoke.biggest_buf_size / my_host_invoke.BYTES_PER_KB
    total_records = json_obj['records']
    if total_records > 0:
        json_obj['data-size-GB'] = total_records * rszkb * 1.0 / KB_PER_GB

    # compute I/O rates

    max_elapsed_time = json_obj['elapsed']
    if max_elapsed_time <= 0.01:  # can't compute rates if it ended too quickly
        json_obj['warnings']['too-brief'] = 0.01  # FIXME: should not hard code
    else:
        json_obj['files-per-sec'] = json_obj['files'] / max_elapsed_time
        if total_records > 0:
            iops = total_records / max_elapsed_time
            json_obj['iops'] = iops
            mb_per_sec = iops * rszkb / 1000.0
            json_obj['MB-per-sec'] = mb_per_sec

    # look for problems

    if prm.host_set:
        missing_thr = (len(prm.host_set) * prm.thread_count) - len(invoke_list)
    else:
        missing_thr = prm.thread_count - len(invoke_list)
    if missing_thr > 0:
        json_obj['warnings']['missing-threads'] = missing_thr

    max_files = my_host_invoke.iterations * len(invoke_list)
    pct_files = 100.0 * json_obj['files'] / max_files
    json_obj['pct-files-processed'] = pct_files
    pct_files_min = prm.pct_files_min
    if pct_files < pct_files_min:
        json_obj['warnings']['not-enough-files-done'] = pct_files_min

    # print everything out here

    print( json.dumps( json_obj, indent=4, separators=(',', ': ')))

def output_results_to_human(invoke_list, prm):
    if len(invoke_list) < 1:
        raise SMFResultException('no pickled invokes read, so no results'
                                 )
    my_host_invoke = invoke_list[0]  # pick a representative one
    total_files = 0
    total_records = 0
    max_elapsed_time = 0.0
    for invk in invoke_list:  # for each parallel SmallfileWorkload

        # add up work that it did
        # and determine time interval over which test ran

        assert isinstance(invk, smallfile.SmallfileWorkload)
        status = 'ok'
        if invk.status:
            status = 'ERR: ' + os.strerror(invk.status)
        fmt = 'host = %s,thr = %s,elapsed = %f'
        fmt += ',files = %d,records = %d,status = %s'
        print(fmt %
              (invk.onhost, invk.tid, invk.elapsed_time,
               invk.filenum_final, invk.rq_final, status))
        total_files += invk.filenum_final
        total_records += invk.rq_final
        max_elapsed_time = max(max_elapsed_time, invk.elapsed_time)

    print('total threads = %d' % len(invoke_list))
    print('total files = %d' % total_files)
    rszkb = my_host_invoke.record_sz_kb
    if rszkb == 0:
        rszkb = my_host_invoke.total_sz_kb
    if rszkb * my_host_invoke.BYTES_PER_KB > my_host_invoke.biggest_buf_size:
        rszkb = my_host_invoke.biggest_buf_size / my_host_invoke.BYTES_PER_KB
    if total_records > 0:
        total_data_gb = total_records * rszkb * 1.0 / KB_PER_GB
        print('total data = %9.3f GB' % total_data_gb)
    if prm.host_set:
        if len(invoke_list) < len(prm.host_set) * prm.thread_count:
            print('WARNING: failed to get some responses from remote hosts')
    elif len(invoke_list) < 1:
            raise SMFResultException('failed to get test response')
    max_files = my_host_invoke.iterations * len(invoke_list)
    pct_files = 100.0 * total_files / max_files
    print('%6.2f%% of requested files processed, minimum is %6.2f' %
          (pct_files, prm.pct_files_min))
    if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly

        print('%f sec elapsed time' % max_elapsed_time)
        files_per_sec = total_files / max_elapsed_time
        print('%f files/sec' % files_per_sec)
        if total_records > 0:
            iops = total_records / max_elapsed_time
            print('%f IOPS' % iops)
            mb_per_sec = iops * rszkb / 1024.0
            print('%f MB/sec' % mb_per_sec)
    if status != 'ok':
        raise SMFResultException(
            'at least one thread encountered error, test may be incomplete')
    if status == 'ok' and pct_files < prm.pct_files_min:
        raise SMFResultException(
            'not enough total files processed, change test parameters')
