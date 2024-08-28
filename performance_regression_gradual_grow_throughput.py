import os
import time
from enum import Enum
from collections import defaultdict

import json
from performance_regression_test import PerformanceRegressionTest, PerformanceTestWorkload
from sdcm.results_analyze import ThroughputLatencyGradualGrowPayloadPerformanceAnalyzer
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.csrangehistogram import CSHistogramTagTypes
from sdcm.utils.decorators import latency_calculator_decorator


class CSPopulateDistribution(Enum):
    GAUSS = "gauss"
    UNIFORM = "uniform"


class PerformanceRegressionGradualGrowThroughputTest(PerformanceRegressionTest):  # pylint: disable=too-many-instance-attributes
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # all parameters were taken from scylla-stress-orch repo
        # Planned data size is 3 TB in total: 1tb per node
        self.CLUSTER_SIZE = self.params.get("n_db_nodes")  # pylint: disable=invalid-name
        self.REPLICATION_FACTOR = 3  # pylint: disable=invalid-name

    def test_mixed_gradual_increase_load(self):  # pylint: disable=too-many-locals
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload with gradual increase load
        """
        self._base_test_workflow(cs_cmd_tmpl=self.params.get('stress_cmd_m'),
                                 test_name="test_mixed_gradual_increase_load (read:50%,write:50%)",
                                 sub_type="mixed")

    def test_write_gradual_increase_load(self):  # pylint: disable=too-many-locals
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a write workload with gradual increase load
        """
        self._base_test_workflow(cs_cmd_tmpl=self.params.get('stress_cmd_w'),
                                 test_name="test_write_gradual_increase_load (100% writes)",
                                 sub_type="write")

    def test_read_gradual_increase_load(self):  # pylint: disable=too-many-locals
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload with gradual increase load
        """
        self._base_test_workflow(cs_cmd_tmpl=self.params.get('stress_cmd_r'),
                                 test_name="test_read_gradual_increase_load (100% reads)",
                                 sub_type="read")

    def _base_test_workflow(self, cs_cmd_tmpl, test_name, sub_type):
        # next 3 lines, is a workaround to have it working inside `latency_calculator_decorator`
        self.cluster = self.db_cluster  # pylint: disable=attribute-defined-outside-init
        self.tester = self  # pylint: disable=attribute-defined-outside-init
        self.monitoring_set = self.monitors  # pylint: disable=attribute-defined-outside-init

        stress_num = 1
        num_loaders = len(self.loaders.nodes)
        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        compaction_strategy = self.params.get('compaction_strategy')
        if sub_type in ["mixed", "read"]:
            self.preload_data(compaction_strategy=compaction_strategy)
            self.wait_no_compactions_running(n=400, sleep_time=120)

            self.run_fstrim_on_all_db_nodes()
        # self.disable_autocompaction_on_all_nodes()
        self.run_gradual_increase_load(stress_cmd_templ=cs_cmd_tmpl,
                                       stress_num=stress_num,
                                       num_loaders=num_loaders,
                                       compaction_strategy=compaction_strategy,
                                       test_name=test_name,
                                       sub_type=sub_type)

    def preload_data(self, compaction_strategy=None):
        population_commands: list = self.params.get("prepare_write_cmd")

        self.log.info("Population c-s commands: %s", population_commands)
        # Check if it should be round_robin across loaders
        params = {}
        stress_queue = []
        if self.params.get('round_robin'):
            self.log.debug('Populating data using round_robin')
            params.update({'stress_num': 1, 'round_robin': True})
        if compaction_strategy:
            self.log.debug('Next compaction strategy will be used %s', compaction_strategy)
            params['compaction_strategy'] = compaction_strategy

        for stress_cmd in population_commands:
            params.update({'stress_cmd': stress_cmd})
            # Run all stress commands
            params.update(dict(stats_aggregate_cmds=False))
            self.log.debug('RUNNING stress cmd: {}'.format(stress_cmd))
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)

        self.log.info("Dataset has been populated")

    @latency_calculator_decorator(legend="Gradual test step")
    def run_step(self, stress_cmd_templ, current_throttle, num_threads, workload):
        results = []
        stress_queue = []
        start_time = time.time()
        for stress_cmd in stress_cmd_templ:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd.replace(
                "$threads", f"{num_threads}").replace("$throttle", f"{current_throttle}")
            params.update({'stress_cmd': stress_cmd_to_run})
            # Run all stress commands
            self.log.debug('RUNNING stress cmd: %s', stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            results.extend(self.get_stress_results(queue=stress, store_results=False))
            self.log.debug("One c-s command results: %s", results[-1])

        end_time = time.time()

        histogram_data_by_interval = self.get_cs_range_histogram_by_interval(stress_operation=workload.value,
                                                                             start_time=start_time,
                                                                             end_time=end_time,
                                                                             tag_type=CSHistogramTagTypes.LATENCY)
        self.log.debug("histogram_data_by_interval: %s", histogram_data_by_interval)
        return results, histogram_data_by_interval

    def calculate_histogram_avg_p99(self, workload, histogram_data_by_interval):
        p99_list = [value['percentile_99'] for interval in histogram_data_by_interval for key, value in interval.items()
                    if key == workload.upper()]
        self.log.debug("P99 from histogram: %s", p99_list)

        return round(sum(p99_list) / len(p99_list), 2) if p99_list else 0

    def drop_keyspace(self):
        self.log.debug(f'Drop keyspace {"keyspace1"}')
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f'DROP KEYSPACE IF EXISTS {"keyspace1"};')

    # pylint: disable=too-many-arguments,too-many-locals
    def run_gradual_increase_load(self, stress_cmd_templ, stress_num, num_loaders, compaction_strategy, test_name, sub_type):  # noqa: PLR0914
        if sub_type in ["read", "mixed"]:
            self.warmup_cache(stress_cmd_templ)

        if not self.exists():
            self.log.debug("Create test statistics in ES")
            self.create_test_stats(sub_type=sub_type, doc_id_with_timestamp=True)
        total_summary = []
        num_threads = self.params["perf_threads"][sub_type]
        throttle_steps = self.params["perf_gradual_throttle_steps"]
        if sub_type not in throttle_steps:
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"Throttle steps for '{sub_type}' test is not defined in 'perf_gradual_throttle_steps' parameter",
                               severity=Severity.CRITICAL).publish()
        workload = getattr(PerformanceTestWorkload, sub_type.upper())

        for throttle_step in throttle_steps[sub_type]:
            self.log.info("Run cs command with rate: %s Kops", throttle_step)
            current_throttle = f"fixed={int(int(throttle_step) // (num_loaders * stress_num))}/s" if throttle_step != "unthrotteled" else ""
            results, histogram_data_by_interval = self.run_step(stress_cmd_templ=stress_cmd_templ, current_throttle=current_throttle,
                                                                num_threads=num_threads, workload=workload)
            self.log.debug("All current results: %s", results)
            summary_result = self._calculate_average_max_latency(results)
            p99th_write_histogram, p99th_read_histogram = 0, 0
            if sub_type in ["read", "mixed"]:
                p99th_read_histogram = self.calculate_histogram_avg_p99(workload="read",
                                                                        histogram_data_by_interval=histogram_data_by_interval)
            if sub_type in ["write"]:
                p99th_write_histogram = self.calculate_histogram_avg_p99(workload="write",
                                                                         histogram_data_by_interval=histogram_data_by_interval)
            es_extra_stats = {"perf_gradual_stats": {throttle_step: {'ops_rate': summary_result["op rate"] * num_loaders,
                                                                     "latency_95th_percentile": {
                                                                         "avg": summary_result["latency 95th percentile"],
                                                                         "max": summary_result["latency 95th percentile max"]},
                                                                     "latency_99th_percentile": {
                                                                         "avg": summary_result["latency 99th percentile"],
                                                                         "max": summary_result["latency 99th percentile max"]},
                                                                     "latency_read_99th_histogram": {"avg": p99th_read_histogram},
                                                                     "latency_write_99th_histogram": {"avg": p99th_write_histogram},
                                                                     "sub_type": sub_type}
                                                     }
                              }
            self.update_test_details(extra_stats=es_extra_stats, scylla_conf=True)
            self.log.debug("C-S results for ops: %s. \n Results: \n %s", throttle_step, summary_result)
            total_summary.append(summary_result)
            if sub_type == "write":
                self.drop_keyspace()
            # We want 3 minutes (180 sec) wait between steps.
            # In case of "write" / "mixed" workflow - wait for compactions finished.
            # In case of "read" workflow -  it just will wait for 3 minutes
            if (wait_time := self.wait_no_compactions_running()[0]) < 180:
                time.sleep(180 - wait_time)

        total_summary_json = json.dumps(total_summary, indent=4, separators=(", ", ": "))
        self.log.debug("---------------------------------")
        self.log.debug("Final table with results: \n %s", total_summary_json)
        self.log.debug("---------------------------------")

        filename = f"{self.logdir}/result_gradual_increase.log"
        with open(filename, "w", encoding="utf-8") as res_file:
            res_file.write(total_summary_json)

        screenshots = self.monitors.get_grafana_screenshots(self.monitors.nodes[0], self.start_time)

        setup_details = {
            "test_id": self.test_id,
            "scylla_version": self.db_cluster.nodes[0].scylla_version_detailed,
            "num_loaders": len(self.loaders.nodes),
            "cluster_size": len(self.db_cluster.nodes),
            "db_instance_type": self.params.get("instance_type_db"),
            "loader_instance_type": self.params.get("instance_type_loader"),
            "scylladb_ami": self.params.get("ami_id_db_scylla"),
            "loader_ami": self.params.get("ami_id_loader"),
            "start_time": self.start_time,
            "screenshots": screenshots,
            "job_url": os.environ.get("BUILD_URL"),
            "shard_aware_driver": self.is_shard_awareness_driver,
        }
        perf_analyzer = ThroughputLatencyGradualGrowPayloadPerformanceAnalyzer(
            es_index=self._test_index,
            es_doc_type=self._es_doc_type,
            email_recipients=self.params.get('email_recipients'))
        self.log.debug("es_index: %s", self._test_index)
        self.log.debug("total_summary: %s", total_summary)
        self.log.debug("setup_details: %s", setup_details)
        is_gce = bool(self.params.get('cluster_backend') == 'gce')
        perf_analyzer.check_regression(test_id=self.test_id, test_name=test_name, subtest_baseline=sub_type,
                                       test_results=total_summary, test_details=setup_details, is_gce=is_gce)

    @staticmethod
    def _calculate_average_max_latency(results):
        status = defaultdict(float).fromkeys(results[0].keys(), 0.0)
        max_latency = defaultdict(list)

        for result in results:
            for key in status:
                try:
                    status[key] += float(result.get(key, 0.0))
                    if key in ["latency 95th percentile", "latency 99th percentile"]:
                        max_latency[f"{key} max"].append(float(result.get(key, 0.0)))
                except ValueError:
                    continue

        for key in status:
            status[key] = round(status[key] / len(results), 2)

        for key, latency in max_latency.items():
            status[key] = max(latency)

        return status

    def warmup_cache(self, stress_cmd_templ):
        stress_queue = []
        for stress_cmd in stress_cmd_templ:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd.replace("$threads", "1000")
            # Run c-s command without throttle
            stress_cmd_to_run = stress_cmd_to_run.replace("$throttle", "")
            # TODO: get "n" from c-s command or do it configurable
            stress_cmd_to_run = stress_cmd_to_run.replace(
                "duration=20m", "n=216666669" if "gauss" in stress_cmd_to_run else "n=20000000")
            # For mixed workflow run read warm up
            stress_cmd_to_run = stress_cmd_to_run.replace("mixed", "read")
            stress_cmd_to_run = stress_cmd_to_run.replace("cl=QUORUM", "cl=ALL")
            params.update({'stress_cmd': stress_cmd_to_run})
            # Run all stress commands
            self.log.debug('RUNNING warm up stress cmd: %s', stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)
