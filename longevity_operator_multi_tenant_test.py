#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2022 ScyllaDB

import concurrent.futures
import copy
import logging
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process, Queue

from longevity_test import LongevityTest

# from sdcm.utils.common import ParallelObject


# pylint: disable=too-many-instance-attributes,too-many-arguments
# pylint: disable=useless-super-delegation
from sdcm.sct_events.events_processes import EventsProcessesRegistry
from sdcm.test_config import TestConfig


class ScyllaClusterStats(LongevityTest):
    def __init__(self, db_cluster, loaders, monitors, prometheus_db, params, test_config, cluster_index):
        self.test_config = TestConfig()
        self._init_logging()
        # self._registry = EventsProcessesRegistry(log_dir=self.logdir)
        self.log.info("Init ScyllaClusterStats for cluster: %s", self.db_cluster.name)
        super().__init__(start_events_device=False)
        self.log.info("prometheus_db: %s", self.prometheus_db)
        self.db_cluster = db_cluster
        self.loaders = loaders
        self.monitors = monitors
        self.prometheus_db = prometheus_db
        self.params = copy.deepcopy(params)
        self._es_doc_type = "test_stats"
        self._stats = self._init_stats()
        self.test_config = test_config
        self._duration = self.params.get(key='test_duration')
        # self.create_stats = self.params.get(key='store_perf_results')
        self.status = "RUNNING"
        self.cluster_index = str(cluster_index)
        self._test_id = self.test_config.test_id() + f"--{cluster_index}"
        self._test_index = self.get_str_index()
        self.create_test_stats()
        self.start_time = self.get_test_start_time()
        self.log.info("prometheus_db: %s", self.prometheus_db)

    def get_str_index(self):
        return f"k8s-longevity-{self.db_cluster.k8s_cluster.tenants_number}-tenants"

    def id(self):  # pylint: disable=invalid-name
        return self.test_config.test_id() + f"-{self._test_index}"

    def __str__(self) -> str:
        return self._test_index + f"--{self.cluster_index}"

    def __repr__(self) -> str:
        return self.__str__()


class LongevityOperatorMulitiTenantTest(LongevityTest):
    scylla_clusters_stats = []
    test_type = "longevity"

    def __init__(self, *args):
        super().__init__(*args, start_events_device=True)

    # def setUp(self):
    #     super().setUp()
        # for i in range(len(self.db_clusters_multitenant)):  # pylint: disable=no-member
        #     self.scylla_clusters_stats.append(ScyllaClusterStats(
        #         db_cluster=self.db_clusters_multitenant[i],  # pylint: disable=no-member
        #         loaders=self.loaders_multitenant[i],  # pylint: disable=no-member
        #         monitors=self.monitors_multitenant[i],  # pylint: disable=no-member
        #         prometheus_db=self.prometheus_db_multitenant[i],  # pylint: disable=no-member
        #         params=self.params,
        #         test_config=self.test_config,
        #         cluster_index=i + 1,
        #     ))
        #     for stress_cmd_param in self.params.stress_cmd_params:
        #         current_stress_cmd = self.params.get(stress_cmd_param)
        #         if not isinstance(current_stress_cmd, list):
        #             continue
        #         # NOTE: 'prepare_write_cmd' is allowed to be list of strs
        #         if stress_cmd_param == 'prepare_write_cmd':
        #             if not all((isinstance(current_stress_cmd_element, list)
        #                         for current_stress_cmd_element in current_stress_cmd)):
        #                 continue
        #         self.scylla_clusters_stats[i].params[stress_cmd_param] = current_stress_cmd[i]

    def run_test_on_one_tenant(self, i):
        self.log.info("Run longevity on cluster %s", self.db_clusters_multitenant[i].name)
        self.scylla_clusters_stats.append(ScyllaClusterStats(
            db_cluster=self.db_clusters_multitenant[i],  # pylint: disable=no-member
            loaders=self.loaders_multitenant[i],  # pylint: disable=no-member
            monitors=self.monitors_multitenant[i],  # pylint: disable=no-member
            prometheus_db=self.prometheus_db_multitenant[i],  # pylint: disable=no-member
            params=self.params,
            test_config=self.test_config,
            cluster_index=i + 1,
        ))
        for stress_cmd_param in self.params.stress_cmd_params:
            current_stress_cmd = self.params.get(stress_cmd_param)
            if not isinstance(current_stress_cmd, list):
                continue
            # NOTE: 'prepare_write_cmd' is allowed to be list of strs
            if stress_cmd_param == 'prepare_write_cmd':
                if not all((isinstance(current_stress_cmd_element, list)
                            for current_stress_cmd_element in current_stress_cmd)):
                    continue
            self.scylla_clusters_stats[i].params[stress_cmd_param] = current_stress_cmd[i]
        self.log.info("Cluster params: %s", self.scylla_clusters_stats[i].params)
        self.log.info("Longevity test with id: %s", id(self.scylla_clusters_stats[i]))
        self.scylla_clusters_stats[i].test_custom_time()

    def test_custom_time(self):

        # def _run_test_on_one_tenant(scylla_cluster_stats):
        #     self.log.info("Cluster params: %s", scylla_cluster_stats.params)
        #     self.log.info("Longevity test with id: %s", id(scylla_cluster_stats))
        #     scylla_cluster_stats.test_custom_time()

        max_workers = len(self.db_clusters_multitenant)
        # results_futures = []
        self.log.info("Starting %d tests worker threads", max_workers)
        # executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        process_pool = ProcessPoolExecutor(max_workers=max_workers)  # pylint: disable=consider-using-with
        results_futures = []
        # queues = [None] * max_workers

        self.log.info("Clusters count: %s", len(self.db_clusters_multitenant))
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as process_pool:
            # self.log.info("Run longevities on all cluster")
            # results_futures = process_pool.map(self.run_test_on_one_tenant,
            #                                    range(len(self.db_clusters_multitenant)))
            for i in range(len(self.db_clusters_multitenant)):
                # p = Process(target=_run_test_on_one_tenant)
                # p.start()

                # for i, scylla_clusters_stat in enumerate(self.scylla_clusters_stats):
                #     q = Queue()
                #     p = Process(target=_run_test_on_one_tenant, args=(scylla_clusters_stat))
                #     p.start()
                # queues[i] = q
                self.log.info("Run longevity on cluster %s", self.db_clusters_multitenant[i].name)
                process = process_pool.submit(self.run_test_on_one_tenant, i)
                self.log.info("Process %s for cluster %s", process, self.db_clusters_multitenant[i].name)
                results_futures.append(process)
                # treads.append(threading.Thread(target=_run_test_on_one_tenant, args=(scylla_clusters_stat,), daemon=True))
                # treads[-1].start()
                # time.sleep(60)

            # for pp_feature in features:
            #     result = pp_feature.result()
            #     self.log.info("result: %s", result)

            # for thread in treads:
            #     thread.join()

            #     results_futures.append(executor.submit(_run_test_on_one_tenant, scylla_clusters_stat))
            #     time.sleep(60)
            #
            # time.sleep(18000)
            results = []
            for future in concurrent.futures.as_completed(results_futures, timeout=18000):  # TODO: get test duration
                self.log.info(f"got exception: {future.exception()}")

        self.log.info("Finished %d tests worker threads. Result: %s", max_workers, results)
        # self.log.info("Finished %d tests worker threads.", max_workers)

        # object_set = ParallelObject(
        #     timeout=14400,
        #     objects=[[scs] for scs in self.scylla_clusters_stats])
        # object_set.run(func=_run_test_on_one_tenant, unpack_objects=True, ignore_exceptions=False)
