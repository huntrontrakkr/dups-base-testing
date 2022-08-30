import datetime
import subprocess
import time
import json
import click
import shlex
import os
import sys
import sqlite3
from sqlite3 import Error
import os
import yaml
from collections.abc import Mapping


# TODO: this should be immutable to be more pythonic
def dict_merge(src1, src2):
    """
        implements a nested/recursive dict merge
        - src2's values take priority
    """

    # Ensure both are dicts
    if not isinstance(src1, dict) or not isinstance(src2, dict):
        return

    # Merge any entry of src2 into src1
    for k in src2:
        # Recurse if key exists in both src1 and src2
        # and they both are dicts
        if k in src1 and isinstance(src1[k], dict) and isinstance(src2[k], Mapping):
            dict_merge(src1[k], src2[k])
        # Otherwise
        # - src1 might not have the field from src2 (k not in src1)
        # - or either src1 or src2 are values
        else:
            # Then take src2's value or dict
            src1[k] = src2[k]

    # Any k in src1 that is not in src2 will keep its content


def merge_runtime_dict(src_filename, dic, dest_filename):
    # Ensure src is a valid file
    if not os.path.isfile(src_filename):
        print("Error: Invalid file '", src_filename, "'", sep="")
        exit(1)

    # Parse config
    src_config = yaml.load(open(src_filename), Loader=yaml.BaseLoader)

    # Merge and replace src1 with dict
    dict_merge(src_config, dic)

    # Output merged config to given dest_filename
    # - creates the file if it does not exist
    yaml.dump(src_config, open(dest_filename, "w+"))


def kill_nextdaemon():
    print("Killing nextdaemon:")
    cmd = 'killall -9 nextdaemon'
    print("    " + cmd)
    res = subprocess.run(cmd, capture_output=True, shell=True)
    print("Try killing elrond:")
    cmd = 'killall -9 elrond'
    subprocess.run(cmd, capture_output=True, shell=True)
    if res.returncode != 0:
        print(f"error in killing nextdaemon")
        print(res.stderr.decode('utf-8'))
        exit(res.returncode)


# TODO: this should be used instead of grepping the nextdaemon.log

def is_runtime_stable():
    try:
        pre = subprocess.run("nextcli runtime status --format=json", capture_output=True, shell=True)
        if pre.returncode != 0:
            print(f"problem with runtime status")
            raise Exception(f"problem with runtime status")

        print(pre.stdout.decode('utf-8'))

        # parse returned json and determine if runtime is stable
        runtime_status = json.loads(pre.stdout.decode('utf-8'))
        # check if key exists
        if 'runtime' in runtime_status and 'stable' in runtime_status['runtime']:
            return runtime_status['runtime']['stable']
    except Exception as e:
        print(e)
        return False
    return False


def load_per_second(conn, window=2):
    cur = conn.cursor()
    cur.execute("""
SELECT funcIdx, bbgIdx, parentIdx,
  CAST(AVG(deltaLoad / deltaTime) AS INTEGER) AS avg_load_per_second,
  CAST(MAX(deltaLoad / deltaTime) AS INTEGER) AS max_load_per_second,
  COUNT(deltaLoad) AS count
FROM
(SELECT funcIdx, bbgIdx, parentIdx,
   ((last_value(load) OVER win) - (first_value(load) OVER win)) AS deltaLoad,
   ((last_value(timestamp) OVER win) - (first_value(timestamp) OVER win)) AS deltaTime
 FROM
 (SELECT funcIdx, bbgIdx, parentIdx, load,
    (timestamp_sec + 1e-9 * timestamp_nsec) as timestamp
  FROM
  mill_counters_tbl
 )
 WINDOW win AS (PARTITION BY funcIdx, bbgIdx ORDER BY timestamp ROWS BETWEEN :window PRECEDING AND CURRENT ROW)
)
WHERE deltaLoad > 0
GROUP BY funcIdx, bbgIdx
""", {"window": window})

    rows = cur.fetchall()
    maxval = 0
    for i in range(0, len(rows)):
        maxval = max(maxval, rows[i][4])
    return maxval


def application_cache_miss_rate(conn):
    cur = conn.cursor()
    cur.execute("""
SELECT
  (1.0 * SUM(miss)) / SUM(total) as miss_rate
FROM
(SELECT
   mep_id,
   SUM(miss) as miss,
   SUM(total) as total
 FROM
 (SELECT
    (((clusterCol * 16) + clusterRow) * 2 + binInCluster) as mep_id,
    miss,
    (miss + fipOrHit) as total
  FROM bin_tbl)
 GROUP BY mep_id)
 """)

    rows = cur.fetchall()

    return rows[0][0]


def application_TLB_miss_rate(conn):
    cur = conn.cursor()
    cur.execute("""
SELECT
  nodeID,
  (1.0 * SUM(miss)) / (SUM(miss) + SUM(hit)) as miss_rate
FROM
(SELECT * FROM
 (SELECT
    nodeID,
    SUM(mmuTLBhit) as hit,
    SUM(mmuTLBmiss) as miss
  FROM
  mep_tbl
  GROUP BY nodeID)
 WHERE hit IS NOT 0 AND miss IS NOT 0)
""")
    rows = cur.fetchall()
    app_tlb = rows[0][1]
    if app_tlb:
        return app_tlb
    else:
        return 0


def launch_nextdaemon(daemon_conf_file, log_file):
    kill_nextdaemon()

    print("Starting nextdaemon")
    cmd = f"nextdaemon --cfg-file {daemon_conf_file} &> {log_file}&"
    print("    " + cmd)
    res = subprocess.run(cmd, capture_output=True, shell=True)
    if res.returncode != 0:
        print(f"error starting nextdaemon")
        print(res.stderr.decode('utf-8'))
        return res.returncode

    check = True
    time0 = time.time()
    while check:
        time.sleep(5)
        res = subprocess.run('grep "Server: listening" nextdaemon.log', shell=True)
        check = res.returncode != 0
        elapsed = time.time() - time0
        if elapsed > 60:
            print(f"problem with launching nextdaemon:")
            print(f"")
            print(f"  >>>>> check the  nextdaemon.log <<<<")
            print(f"")
            kill_nextdaemon()
            raise Exception(f"problem with launching nextdaemon")

    return 0


def enable_handoff():
    print("Enabling handoff")
    res = subprocess.run("nextcli runtime handoff enable", shell=True)
    if res.returncode != 0:
        print(f"problem with handoff")
        raise Exception(f"problem with handoff")


def save_runtime(path):
    print("Saving runtime")
    res = subprocess.run(f"nextcli runtime handoff {path}", shell=True)
    if res.returncode != 0:
        print(f"problem with saving runtime")
        raise Exception(f"problem with saving runtime")


def load_runtime(path):
    print("Loading runtime")
    res = subprocess.run(f"nextcli runtime handoff load {path}", shell=True)
    if res.returncode != 0:
        print(f"problem with loading runtime")
        raise Exception(f"problem with loading runtime")


def generate_mills(daemon_conf_file, log_file, nsbench, kernel, perf_args, mill_args, mill_threads="8",
                   end_if_stable=True):
    launch_nextdaemon(daemon_conf_file, log_file)
    print("Generating mills:")
    cmd = f"nextloader -- {nsbench}  --kernel={kernel} --num-threads={mill_threads} {mill_args}"
    print("    " + cmd)
    res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # if end_if_stable is true, then kill nextloader early:
    time0 = time.time()
    poll = None
    loop_till_stable = not is_runtime_stable()
    while loop_till_stable:
        loop_till_stable = not is_runtime_stable()
        time.sleep(5)
        elapsed = time.time() - time0
        poll = res.poll()
        if end_if_stable and is_runtime_stable() and poll is None:
            print("Runtime is stable - killing nextloader early")
            res.kill()
            break
        if elapsed > 60:
            print(f"problem with generating mills (timeout){os.linesep}")
            print(f"  >>>>> check the nextloader.log <<<<{os.linesep}")
            raise Exception(f"problem with generating mills")
        elif poll is not None and poll != 0:
            print(f"problem with generating mills (nextloader exited with {poll}){os.linesep}")
            print(f"  >>>>> check the nextloader.log <<<<{os.linesep}")
            raise Exception(f"problem with generating mills")
        elif poll is not None and poll == 0 and not is_runtime_stable():
            print(
                f"problem with generating mills (nextloader exited with {poll} but runtime is not stable){os.linesep}")
            print(f"  >>>>> check the nextloader.log <<<<{os.linesep}")
            raise Exception(f"problem with generating mills")


def perf_run(nsbench, kernel, perf_args, db_filename, perf_threads="dynamic"):
    print("Performance Run:")
    cmd = f"{nsbench} --kernel={kernel} --num-threads={perf_threads} {perf_args}"
    print("    " + cmd)
    res = subprocess.run(cmd, capture_output=True, shell=True)
    nextmonitor = subprocess.Popen(f"nextmonitor --db-filename {db_filename}", stderr=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL)
    if res.returncode != 0:
        print(f"error in performance generation")
        print(res.stderr.decode('utf-8'))
        nextmonitor.kill()
        raise Exception(f"problem with handoff")

    print(res.stdout.decode('utf-8').strip())
    nextmonitor.kill()

    return json.loads(res.stdout.decode('utf-8'))


n_rows = 8
n_col = 4
nsbench = "/space/users/wadeh/sw/nsbench/build/bin/nsbench"
perf_args = "--N=2000000000 --unroll=2 --mode=unique"
mill_args = "--N=2000000000 --ntimes=4000 --unroll=2 --mode=unique"
kernel = "dot"
daemon_conf_file = "/space/users/wadeh/sw/nsbench/next_runtime.conf"
mill_threads = "8"
perf_threads = "dynamic"


# note: keep to 8 gigs.
@click.command()
@click.option('--nrow', default=n_rows, help='Number of rows to offset')
@click.option('--ncol', default=n_col, help='Number of columns')
@click.option('--perf-threads', default=perf_threads, help='Number of threads')
@click.option('--mill-threads', default=mill_threads, help='Number of threads')
@click.option('--perf-args', default=perf_args, help='Arguments for performance run')
@click.option('--mill-args', default=mill_args, help='Arguments for mill generation')
@click.option('--kernel', default=kernel, help='Kernel')
@click.option('--nextdaemon-cfg', default=daemon_conf_file, help='root nextdaemon configuration file')
@click.option('--nsbench', default=nsbench, help='nsbench executable location')
def main(n_rows, n_col, perf_threads, mill_threads, perf_args, mill_args, kernel, nextdaemon_cfg):
    root = f"./{kernel}_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.mkdir(root)
    gen_run_folder = os.path.join(root, "gen_run")
    os.mkdir(gen_run_folder)

    launch_nextdaemon(nextdaemon_cfg, os.path.join(gen_run_folder, "nextdaemon.log"))

    # first, generate the mills, then save the runtime
    generate_mills(daemon_conf_file, os.path.join(gen_run_folder, "mill_gen.log"), nsbench, kernel, perf_args,
                   mill_args, mill_threads)
    save_runtime(os.path.join(gen_run_folder, "runtime"))

    # now, load the runtime, then run the performance run for each configuration
    subfolders = {f"row{i}_col{j}": (i, j) for i in range(0, n_rows) for j in range(0, n_col)}
    for subfolder in subfolders.keys():
        # make the configuration file with the appropriate offset
        os.mkdir(os.path.join(root, subfolder))
        new_conf = os.path.join(gen_run_folder, subfolder, "nextdaemon.conf")
        modification = {
            'projection': {
                'experimental': {
                    'offset': {
                        'row': subfolders[subfolder][0],
                        'col': subfolders[subfolder][1]
                    },
                    # 'relocate_on_load': {}
                }
            }
        }
        merge_runtime_dict(nextdaemon_cfg, modification, new_conf)
        launch_nextdaemon(new_conf, os.path.join(root, subfolder, "nextdaemon.log"))
        load_runtime(os.path.join(gen_run_folder, "runtime"))
        enable_handoff()
        monitor_log_path = os.path.join(root, subfolder, "monitor.sqlite")
        results = perf_run(nsbench, kernel, perf_args, monitor_log_path, perf_threads)
        with open(os.path.join(root, subfolder, "results.json"), "w") as f:
            json.dump(results, f)

        print(f"finished {subfolder} perf run")

        # query sqlite database and generate metrics
        # TODO


if __name__ == "__main__":
    main()
