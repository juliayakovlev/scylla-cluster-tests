import os
from enum import Enum
from collections import defaultdict

import json
from performance_regression_test import PerformanceRegressionTest
from sdcm.results_analyze import ThroughputLatencyGradualGrowPayloadPerformanceAnalyzer, PP


class CSPopulateDistribution(Enum):
    GAUSS = "gauss"
    UNIFORM = "uniform"


class PerformanceRegressionGradualGrowThroughputTest(PerformanceRegressionTest):  # pylint: disable=too-many-instance-attributes
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # all parameters were taken from scylla-stress-orch repo
        # Planned data size is 3 TB in total: 1tb per node
        self.NUM_THREADS = 700  # pylint: disable=invalid-name
        self.CLUSTER_SIZE = self.params.get("n_db_nodes")  # pylint: disable=invalid-name
        self.REPLICATION_FACTOR = 3  # pylint: disable=invalid-name
        # Planned data size is 3 TB in total: 1tb per node
        self.DATASET_SIZE = 10 * 1024 * 1024 * 1024  # pylint: disable=invalid-name
        # self.DATASET_SIZE = 3000 * 1024 * 1024 * 1024  # pylint: disable=invalid-name
        # expected row size (was calculated by dev team ~313 bytes)
        self.ROW_SIZE_BYTES = 210 * 1024 * 1024 * 1024 / 720_000_000  # pylint: disable=invalid-name
        self.ROW_COUNT = int(self.DATASET_SIZE / self.ROW_SIZE_BYTES /  # pylint: disable=invalid-name
                             self.REPLICATION_FACTOR)
        # How many standard deviations should span the cluster's memory
        self.CONFIDENCE = 6  # pylint: disable=invalid-name
        # instance i3.4xlarge
        self.INSTANCE_MEMORY_GB = 128  # pylint: disable=invalid-name
        self.GAUSS_CENTER = self.ROW_COUNT // 2  # pylint: disable=invalid-name
        self.GAUSS_SIGMA = int(self.INSTANCE_MEMORY_GB * self.CLUSTER_SIZE * 1024 * 1024 *  # pylint: disable=invalid-name
                               1024 // (self.CONFIDENCE * self.ROW_SIZE_BYTES * self.REPLICATION_FACTOR))

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
        stress_num = 1
        num_loaders = len(self.loaders.nodes)
        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        compaction_strategy = self.params.get('compaction_strategy')
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

    # pylint: disable=too-many-arguments,too-many-locals
    def run_gradual_increase_load(self, stress_cmd_templ, stress_num, num_loaders, compaction_strategy, test_name, sub_type):
        self.warmup_cache(compaction_strategy)
        if not self.exists():
            self.create_test_stats(sub_type=sub_type, doc_id_with_timestamp=True)
        total_summary = []
        max_ops = self.params.get(f"perf_max_ops_{sub_type}")
        throttle_steps = self.params.get("perf_throttle_steps")

        throttle_step = 100
        # for throttle_step in throttle_steps:
        for num_threads in [500, 500, 700, 700, 900, 900, 1000, 1000, 1100, 1100, 1200, 1200]:
            current_ops = int(max_ops * throttle_step / 100) if throttle_step < 100 else max_ops
            self.log.info("Run cs command with rate: %s Kops", current_ops)
            current_throttle = f"throttle={int(current_ops // (num_loaders * stress_num))}/s" if current_ops < max_ops else ""
            stress_queue = []
            for stress_cmd in stress_cmd_templ:
                params = {"round_robin": True, "stats_aggregate_cmds": False}
                # stress_cmd_to_run = stress_cmd.replace("$threads", f"{self.NUM_THREADS}").replace(
                stress_cmd_to_run = stress_cmd.replace("$threads", f"{num_threads}").replace(
                    "$throttle", f"{current_throttle}")
                params.update({'stress_cmd': stress_cmd_to_run})
                # Run all stress commands
                self.log.debug('RUNNING stress cmd: %s', stress_cmd_to_run)
                stress_queue.append(self.run_stress_thread(**params))

            results = []
            for stress in stress_queue:
                results.extend(self.get_stress_results(queue=stress, store_results=False))
                self.log.debug("One c-s command results: %s", results[-1])
                self.log.debug("All current results: %s", results)
            summary_result = self._calculate_average_max_latency(results)
            summary_result["ops"] = current_ops
            # TODO: remove next line
            summary_result["op_rate"] = summary_result["op rate"] * num_loaders
            es_extra_stats = {'ops_expected': current_ops,
                              'op_rate': summary_result["op rate"] * num_loaders,
                              "latency_95th_percentile_avg": summary_result["latency 95th percentile"],
                              "latency_95th_percentile_max": summary_result["latency 95th percentile max"],
                              "latency_99th_percentile_avg": summary_result["latency 99th percentile"],
                              "latency_99th_percentile_max": summary_result["latency 99th percentile max"],
                              }
            self.log.debug("Loaders amount: %s; Threads number: %s; Stat:\n%s",
                           num_loaders, num_threads, PP.pformat(es_extra_stats))
            self.update_test_details(extra_stats=es_extra_stats, scylla_conf=True)
            self.log.debug("C-S results for ops: %s. \n Results: \n %s", current_ops, summary_result)
            total_summary.append(summary_result)

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
        perf_analyzer.check_regression(test_id=self.test_id, test_name=test_name,
                                       test_results=total_summary, test_details=setup_details, is_gce=is_gce)

    def get_cs_distribution(self):
        popuplation_distribution = CSPopulateDistribution(self.params.get("cs_populating_distribution"))
        if popuplation_distribution == CSPopulateDistribution.GAUSS:
            return f"GAUSSIAN(1..{self.ROW_COUNT},{self.GAUSS_CENTER},{self.GAUSS_SIGMA})"
        elif popuplation_distribution == CSPopulateDistribution.UNIFORM:
            return f"UNIFORM(1..{self.ROW_COUNT})"
        else:
            self.log.error("Unsupported cs population distribution")
            return ""

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

    def warmup_cache(self, compaction_strategy):
        cmd = f"cassandra-stress read no-warmup cl=QUORUM duration=20m -pop 'dist=gauss(1..650000004,54166668,542209)' -mode native cql3 -rate 'threads=1000' -col 'size=FIXED(1024) n=FIXED(1)'"  # pylint: disable=line-too-long
        # cmd = f"cassandra-stress read no-warmup cl=QUORUM duration=180m -pop 'dist={self.get_cs_distribution()}' -mode native cql3 -rate 'threads=500 throttle=35000/s'"  # pylint: disable=line-too-long
        stress_queue = self.run_stress_cassandra_thread(
            stress_cmd=cmd,
            stress_num=1,
            compaction_strategy=compaction_strategy,
            stats_aggregate_cmds=False,
            round_robin=True
        )
        self.get_stress_results(stress_queue, store_results=False)
