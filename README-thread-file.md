Traceback (most recent call last):
  File "/home/perftestuser/smallfile/smallfile_remote.py", line 48, in <module>
    run_workload()
  File "/home/perftestuser/smallfile/smallfile_remote.py", line 39, in run_workload
    return multi_thread_workload.run_multi_thread_workload(params)
  File "/home/perftestuser/smallfile/multi_thread_workload.py", line 64, in run_multi_thread_workload
    thread_list = create_worker_list(prm)
  File "/home/perftestuser/smallfile/multi_thread_workload.py", line 35, in create_worker_list
    ensure_deleted(nextinv.gen_thread_ready_fname(nextinv.tid))
  File "/home/perftestuser/smallfile/smallfile.py", line 133, in ensure_deleted
    % (fn, str(e)))
Exception: exception while ensuring /var/tmp/thread_ready.00.tmp deleted: [Errno 1] Operation not permitted: '/var/tmp/thread_ready.00.tmp'