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

import copy
import logging
import time

from longevity_test import LongevityTest

from sdcm.utils.common import ParallelObject


# pylint: disable=too-many-instance-attributes
class ScyllaClusterStats(LongevityTest):
    _testMethodName = "runTest"

    # pylint: disable=too-many-arguments,super-init-not-called
    def __init__(self, db_cluster, loaders, monitors, prometheus_db, params, test_config, cluster_index):
        self.db_cluster = db_cluster
        self.loaders = loaders
        self.monitors = monitors
        self.prometheus_db = prometheus_db
        self.params = copy.deepcopy(params)
        self.test_config = test_config
        self._duration = self.params.get(key='test_duration')
        self.log = logging.getLogger(self.__class__.__name__)
        self.create_stats = self.params.get(key='store_perf_results')
        self.status = "RUNNING"
        self.cluster_index = str(cluster_index)
        self._test_id = self.test_config.test_id() + f"--{cluster_index}"
        self._test_index = self.get_str_index()
        self.start_time = time.time()
        self._duration = self.params.get(key='test_duration')

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
    _testMethodName = "runTest"
    # test_type = "longevity"

    # def __init__(self, *args):
    #     super().__init__(*args, start_events_device=True)

    def setUp(self):
        super().setUp()
        for i, db_cluster in enumerate(self.db_clusters_multitenant):  # pylint: disable=no-member
            self.scylla_clusters_stats.append(ScyllaClusterStats(
                db_cluster=db_cluster,
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

    def test_custom_time(self):
        def _run_test_on_one_tenant(scylla_cluster_stats):
            self.log.info("Longevity test for cluster %s with parameters: %s", scylla_cluster_stats.db_cluster,
                          scylla_cluster_stats.params)
            scylla_cluster_stats.test_custom_time()

        self.log.info("Starting tests worker threads")

        self.log.info("Clusters count: %s", self.k8s_cluster.tenants_number)
        object_set = ParallelObject(
            timeout=int(self.test_duration) * 60,
            objects=[[scs] for scs in self.scylla_clusters_stats],
            num_workers=self.k8s_cluster.tenants_number
        )
        object_set.run(func=_run_test_on_one_tenant, unpack_objects=True, ignore_exceptions=False)
