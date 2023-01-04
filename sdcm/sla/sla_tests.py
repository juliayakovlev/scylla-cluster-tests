#!/usr/bin/env python
import logging
import time
import traceback
import uuid

from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestStepEvent
from sdcm.sla.libs.sla_utils import SlaUtils
from sdcm.utils.loader_utils import DEFAULT_USER, DEFAULT_USER_PASSWORD, SERVICE_LEVEL_NAME_TEMPLATE
from test_lib.sla import create_sla_auth, ServiceLevel

LOGGER = logging.getLogger(__name__)


class Steps(SlaUtils):
    # pylint: disable=too-many-arguments
    def run_stress_and_validate_scheduler_runtime_during_load(self, tester, read_cmds, prometheus_stats, read_roles,
                                                              stress_queue, sleep=600):
        # pylint: disable=not-context-manager
        with TestStepEvent(step="Run stress command and validate scheduler runtime during load") as wp_event:
            try:
                start_time = time.time() + 60
                # pylint: disable=protected-access
                tester._run_all_stress_cmds(stress_queue, params={'stress_cmd': read_cmds, 'round_robin': True})
                time.sleep(sleep)
                end_time = time.time()

                self.validate_scheduler_runtime(start_time=start_time,
                                                end_time=end_time,
                                                read_users=read_roles,
                                                prometheus_stats=prometheus_stats,
                                                db_cluster=tester.db_cluster)
                return None
            except Exception as details:  # pylint: disable=broad-except
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    # pylint: disable=too-many-arguments
    def alter_sl_and_validate_scheduler_runtime(self, tester, service_level, new_shares, read_roles, prometheus_stats,
                                                sleep=600):
        # pylint: disable=not-context-manager
        with TestStepEvent(step=f"Alter shares from {service_level.shares} to {new_shares} Service "
                                f"Level {service_level.name} and validate scheduler runtime "
                                f"during load") as wp_event:
            try:
                service_level.alter(new_shares=new_shares)
                start_time = time.time() + 60
                time.sleep(sleep)
                end_time = time.time()
                self.validate_scheduler_runtime(start_time=start_time,
                                                end_time=end_time,
                                                read_users=read_roles,
                                                prometheus_stats=prometheus_stats,
                                                db_cluster=tester.db_cluster)
                return None
            except Exception as details:  # pylint: disable=broad-except
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    # pylint: disable=too-many-arguments
    @staticmethod
    def detach_service_level_and_run_load(sl_for_detach, role_with_sl_to_detach, sleep=600):
        # pylint: disable=not-context-manager
        with TestStepEvent(step=f"Detach service level {sl_for_detach.name} with {sl_for_detach.shares} shares from "
                                f"{role_with_sl_to_detach.name}.") as wp_event:
            try:
                role_with_sl_to_detach.detach_service_level()
                time.sleep(sleep)
                return None
            except Exception as details:  # pylint: disable=broad-except
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    # pylint: disable=too-many-arguments
    @staticmethod
    def drop_service_level_and_run_load(sl_for_drop, role_with_sl_to_drop, sleep=600):
        # pylint: disable=not-context-manager
        with TestStepEvent(step=f"Drop service level {sl_for_drop.name} with {role_with_sl_to_drop.name}.") as wp_event:
            try:
                sl_for_drop.drop()
                role_with_sl_to_drop.reset_service_level()
                time.sleep(sleep)
                return None
            except Exception as details:  # pylint: disable=broad-except
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    # pylint: disable=too-many-arguments
    def attach_sl_and_validate_scheduler_runtime(self, tester, new_service_level, role_for_attach,
                                                 read_roles, prometheus_stats, sleep=600):
        # pylint: disable=not-context-manager
        with TestStepEvent(step=f"Attach service level {new_service_level.name} with "
                                f"{new_service_level.shares} shares to {role_for_attach.name}. "
                                f"Validate scheduler runtime during load") as wp_event:
            try:
                role_for_attach.attach_service_level(new_service_level)
                start_time = time.time() + 60
                time.sleep(sleep)
                end_time = time.time()
                self.validate_scheduler_runtime(start_time=start_time,
                                                end_time=end_time,
                                                read_users=read_roles,
                                                prometheus_stats=prometheus_stats,
                                                db_cluster=tester.db_cluster)
                return None
            except Exception as details:  # pylint: disable=broad-except
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event


class SlaTests(Steps):
    STRESS_READ_CMD = 'cassandra-stress read cl=ALL duration={duration} -mode cql3 native user={user} ' \
                      'password={password} -rate threads={threads} -pop {pop}'

    @staticmethod
    def refresh_role_in_list(role_to_refresh, read_roles):
        for i, role in enumerate(read_roles):
            if role["role"] == role_to_refresh:
                read_roles[i]["service_level"] = role_to_refresh.attached_service_level

    # pylint: disable=too-many-locals
    def test_increase_shares_by_attach_another_sl_during_load(self, tester, prometheus_stats, num_of_partitions):
        low_share = 20
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = create_sla_auth(session=session, shares=low_share, index=auth_entity_name_index)
            role_high = create_sla_auth(session=session, shares=high_share, index=auth_entity_name_index)

            stress_duration = 25
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions)
                         ]
            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_runtime_during_load(tester=tester, read_cmds=read_cmds,
                                                                               prometheus_stats=prometheus_stats,
                                                                               read_roles=read_roles,
                                                                               stress_queue=stress_queue))
                # Create new role and attach it instead of detached
                new_sl = ServiceLevel(session=session,
                                      name=SERVICE_LEVEL_NAME_TEMPLATE % ('80', auth_entity_name_index),
                                      shares=80).create()

                error_events.append(
                    self.attach_sl_and_validate_scheduler_runtime(tester=tester,
                                                                  new_service_level=new_sl,
                                                                  role_for_attach=role_high,
                                                                  read_roles=read_roles,
                                                                  prometheus_stats=prometheus_stats,
                                                                  sleep=600))
            finally:
                for stress in stress_queue:
                    tester.verify_stress_thread(cs_thread_pool=stress)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events  # pylint: disable=lost-exception

    # pylint: disable=too-many-locals
    def test_increase_shares_during_load(self, tester, prometheus_stats, num_of_partitions):
        low_share = 20
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = create_sla_auth(session=session, shares=low_share, index=auth_entity_name_index)
            role_high = create_sla_auth(session=session, shares=high_share, index=auth_entity_name_index)

            stress_duration = 25
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions)
                         ]
            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_runtime_during_load(tester=tester, read_cmds=read_cmds,
                                                                               prometheus_stats=prometheus_stats,
                                                                               read_roles=read_roles,
                                                                               stress_queue=stress_queue))
                error_events.append(
                    self.alter_sl_and_validate_scheduler_runtime(tester=tester,
                                                                 service_level=role_low.attached_service_level,
                                                                 new_shares=900, read_roles=read_roles,
                                                                 prometheus_stats=prometheus_stats))
            finally:
                for stress in stress_queue:
                    tester.verify_stress_thread(cs_thread_pool=stress)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events  # pylint: disable=lost-exception

    # pylint: disable=too-many-locals
    def test_decrease_shares_during_load(self, tester, prometheus_stats, num_of_partitions):
        low_share = 800
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = create_sla_auth(session=session, shares=low_share, index=auth_entity_name_index)
            role_high = create_sla_auth(session=session, shares=high_share, index=auth_entity_name_index)

            stress_duration = 25
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions)
                         ]
            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_runtime_during_load(tester=tester, read_cmds=read_cmds,
                                                                               prometheus_stats=prometheus_stats,
                                                                               read_roles=read_roles,
                                                                               stress_queue=stress_queue))
                error_events.append(
                    self.alter_sl_and_validate_scheduler_runtime(tester=tester,
                                                                 service_level=role_low.attached_service_level,
                                                                 new_shares=100, read_roles=read_roles,
                                                                 prometheus_stats=prometheus_stats))

            finally:
                for stress in stress_queue:
                    tester.verify_stress_thread(cs_thread_pool=stress)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events  # pylint: disable=lost-exception

    # pylint: disable=too-many-locals
    def test_replace_service_level_using_detach_during_load(self, tester, prometheus_stats, num_of_partitions):
        low_share = 250
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = create_sla_auth(session=session, shares=low_share, index=auth_entity_name_index)
            role_high = create_sla_auth(session=session, shares=high_share, index=auth_entity_name_index)

            stress_duration = 35
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions)
                         ]

            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_runtime_during_load(tester=tester, read_cmds=read_cmds,
                                                                               prometheus_stats=prometheus_stats,
                                                                               read_roles=read_roles,
                                                                               stress_queue=stress_queue))

                error_events.append(
                    self.detach_service_level_and_run_load(sl_for_detach=role_high.attached_service_level,
                                                           role_with_sl_to_detach=role_high,
                                                           sleep=600))
                self.refresh_role_in_list(role_to_refresh=role_high, read_roles=read_roles)

                # Create new role and attach it instead of detached
                new_sl = ServiceLevel(session=session,
                                      name=SERVICE_LEVEL_NAME_TEMPLATE % ('50', auth_entity_name_index),
                                      shares=50).create()

                error_events.append(
                    self.attach_sl_and_validate_scheduler_runtime(tester=tester,
                                                                  new_service_level=new_sl,
                                                                  role_for_attach=role_high,
                                                                  read_roles=read_roles,
                                                                  prometheus_stats=prometheus_stats,
                                                                  sleep=600))

            finally:
                for stress in stress_queue:
                    tester.verify_stress_thread(cs_thread_pool=stress)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events  # pylint: disable=lost-exception

    # pylint: disable=too-many-locals
    def test_replace_service_level_using_drop_during_load(self, tester, prometheus_stats, num_of_partitions):
        low_share = 250
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = create_sla_auth(session=session, shares=low_share, index=auth_entity_name_index)
            role_high = create_sla_auth(session=session, shares=high_share, index=auth_entity_name_index)

            stress_duration = 35
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions)
                         ]

            read_roles = [{"role": role_low, "service_level": role_low.attached_service_level},
                          {"role": role_high, "service_level": role_high.attached_service_level}]

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_runtime_during_load(tester=tester, read_cmds=read_cmds,
                                                                               prometheus_stats=prometheus_stats,
                                                                               read_roles=read_roles,
                                                                               stress_queue=stress_queue))

                error_events.append(
                    self.drop_service_level_and_run_load(sl_for_drop=role_low.attached_service_level,
                                                         role_with_sl_to_drop=role_low,
                                                         sleep=600))
                self.refresh_role_in_list(role_to_refresh=role_low, read_roles=read_roles)

                # Create new role and attach it instead of dropped
                new_sl = ServiceLevel(session=session,
                                      name=SERVICE_LEVEL_NAME_TEMPLATE % ('800', auth_entity_name_index),
                                      shares=800).create()

                error_events.append(
                    self.attach_sl_and_validate_scheduler_runtime(tester=tester,
                                                                  new_service_level=new_sl,
                                                                  role_for_attach=role_low,
                                                                  read_roles=read_roles,
                                                                  prometheus_stats=prometheus_stats,
                                                                  sleep=600))

            finally:
                for stress in stress_queue:
                    tester.verify_stress_thread(cs_thread_pool=stress)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events  # pylint: disable=lost-exception

    # pylint: disable=too-many-locals
    def test_seven_sl_with_max_shares_during_load(self, tester, prometheus_stats, num_of_partitions):
        error_events = []
        stress_queue = []
        auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]
        every_role_shares = 1000
        stress_duration = 20
        read_cmds = []
        read_roles = []

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            roles = []
            for _ in range(7):
                auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]
                roles.append(create_sla_auth(session=session, shares=every_role_shares, index=auth_entity_name_index))
                read_cmds.append(self.define_read_cassandra_stress_command(role=roles[-1],
                                                                           load_type=self.MIXED_LOAD,
                                                                           c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                           threads=50,
                                                                           stress_duration_min=stress_duration,
                                                                           num_of_partitions=num_of_partitions,
                                                                           max_rows_for_read=num_of_partitions))

                read_roles.append({"role": roles[-1], 'service_level': roles[-1].attached_service_level})

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_runtime_during_load(tester=tester,
                                                                               read_cmds=read_cmds,
                                                                               prometheus_stats=prometheus_stats,
                                                                               read_roles=read_roles,
                                                                               stress_queue=stress_queue))

            finally:
                for stress in stress_queue:
                    tester.verify_stress_thread(cs_thread_pool=stress)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events  # pylint: disable=lost-exception

    # pylint: disable=too-many-locals
    def test_seven_sls_with_different_shares_during_load(self, tester, prometheus_stats, num_of_partitions):
        error_events = []
        stress_queue = []
        auth_entity_name_index = str(uuid.uuid1()).split("-", maxsplit=1)[0]
        role_shares = [1000, 100, 200, 300, 500, 700, 800]
        stress_duration = 20
        read_cmds = []
        read_roles = []

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            roles = []
            for shares in role_shares:
                roles.append(create_sla_auth(session=session, shares=shares, index=auth_entity_name_index))
                read_cmds.append(self.define_read_cassandra_stress_command(role=roles[-1],
                                                                           load_type=self.MIXED_LOAD,
                                                                           c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                           threads=50,
                                                                           stress_duration_min=stress_duration,
                                                                           num_of_partitions=num_of_partitions,
                                                                           max_rows_for_read=num_of_partitions))

                read_roles.append({"role": roles[-1], 'service_level': roles[-1].attached_service_level})

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_runtime_during_load(tester=tester,
                                                                               read_cmds=read_cmds,
                                                                               prometheus_stats=prometheus_stats,
                                                                               read_roles=read_roles,
                                                                               stress_queue=stress_queue))

            finally:
                for stress in stress_queue:
                    tester.verify_stress_thread(cs_thread_pool=stress)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events  # pylint: disable=lost-exception
