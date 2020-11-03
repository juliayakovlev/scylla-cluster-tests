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
# Copyright (c) 2020 ScyllaDB

import logging
import concurrent.futures
import os
import uuid
import random
import json
import time

from sdcm.sct_events import GeminiEvent, GeminiLogEvent, Severity
from sdcm.utils.common import FileFollowerThread

LOGGER = logging.getLogger(__name__)


class NotGeminiErrorResult():  # pylint: disable=too-few-public-methods
    def __init__(self, error):
        self.exited = 1
        self.stdout = "n/a"
        self.stderr = str(error)


class GeminiEventsPublisher(FileFollowerThread):
    def __init__(self, node, gemini_log_filename, verbose=False):
        super().__init__()
        self.gemini_log_filename = gemini_log_filename
        self.node = str(node)
        self.verbose = verbose

    def run(self):
        while not self.stopped():
            if not os.path.isfile(self.gemini_log_filename):
                time.sleep(0.5)
                continue
            for line_number, line in enumerate(self.follow_file(self.gemini_log_filename), start=1):
                GeminiLogEvent(verbose=self.verbose).add_info_and_publish(self.node, line, line_number)
                if self.stopped():
                    break


class GeminiStressThread():  # pylint: disable=too-many-instance-attributes

    def __init__(self, test_cluster, oracle_cluster, loaders, gemini_cmd, timeout=None, outputdir=None, params=None):  # pylint: disable=too-many-arguments
        self.loaders = loaders
        self.gemini_cmd = gemini_cmd
        self.test_cluster = test_cluster
        self.oracle_cluster = oracle_cluster
        self.timeout = timeout
        self.result_futures = []
        self.outputdir = outputdir
        self.gemini_commands = []
        self._gemini_result_file = None
        self.params = params if params else {}

    @property
    def gemini_result_file(self):
        if not self._gemini_result_file:
            self._gemini_result_file = os.path.join(self.loaders.gemini_base_path,
                                                    "gemini_result_{}.log".format(uuid.uuid4()))
        return self._gemini_result_file

    def _generate_gemini_command(self):
        seed = self.params.get('gemini_seed')
        table_options = self.params.get('gemini_table_options')
        if not seed:
            seed = random.randint(1, 100)
        test_node = random.choice(self.test_cluster.nodes)
        oracle_node = random.choice(self.oracle_cluster.nodes) if self.oracle_cluster else None

        cmd = "/$HOME/{} --test-cluster={} --outfile {} --seed {} ".format(self.gemini_cmd.strip(),
                                                                           test_node.ip_address,
                                                                           self.gemini_result_file,
                                                                           seed)
        if oracle_node:
            cmd += "--oracle-cluster={} ".format(oracle_node.ip_address)
        if table_options:
            cmd += " ".join([f"--table-options \"{table_opt}\"" for table_opt in table_options])
        self.gemini_commands.append(cmd)
        return cmd

    def run(self):

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.loaders.nodes))
        for loader_idx, loader in enumerate(self.loaders.nodes):
            self.result_futures.append(executor.submit(self._run_gemini, loader, loader_idx))

        return self

    def _run_gemini(self, node, loader_idx):
        logdir = os.path.join(self.outputdir, node.name)
        os.makedirs(logdir, exist_ok=True)

        log_file_name = os.path.join(logdir,
                                     'gemini-l%s-%s.log' %
                                     (loader_idx, uuid.uuid4()))
        gemini_cmd = self._generate_gemini_command()

        GeminiEvent(type='start', cmd=gemini_cmd)
        try:
            with GeminiEventsPublisher(node=node, gemini_log_filename=log_file_name):

                result = node.remoter.run(cmd=gemini_cmd,
                                          timeout=self.timeout,
                                          ignore_status=False,
                                          log_file=log_file_name)
                # sleep to gather all latest log messages
                time.sleep(5)
        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error(details)
            result = getattr(details, "result", NotGeminiErrorResult(details))

        GeminiEvent(type='finish', cmd=gemini_cmd,
                    result={'exit_code': result.exited,
                            'stdout': result.stdout,
                            'stderr': result.stderr})

        return node, result, self.gemini_result_file

    def get_gemini_results(self):
        results = []
        command_result = []

        LOGGER.debug('Wait for %s gemini threads results', len(self.loaders.nodes))
        for future in concurrent.futures.as_completed(self.result_futures, timeout=self.timeout):
            results.append(future.result())

        for node, _, result_file in results:

            local_gemini_result_file = os.path.join(node.logdir, os.path.basename(result_file))
            node.remoter.receive_files(src=result_file, dst=local_gemini_result_file)
            with open(local_gemini_result_file) as local_file:
                content = local_file.read()
                res = self._parse_gemini_summary_json(content)
                if res:
                    command_result.append(res)

        return command_result

    @staticmethod
    def verify_gemini_results(results):

        stats = {'status': None, 'results': [], 'errors': {}}
        if not results:
            LOGGER.error('Gemini results are not found')
            stats['status'] = 'FAILED'
        else:
            for res in results:
                stats['results'].append(res)
                for err_type in ['write_errors', 'read_errors', 'errors']:
                    if res.get(err_type, None):
                        LOGGER.error("Gemini {} errors: {}".format(err_type, res[err_type]))
                        stats['status'] = 'FAILED'
                        stats['errors'][err_type] = res[err_type]
        if not stats['status']:
            stats['status'] = "PASSED"

        return stats

    @staticmethod
    def _parse_gemini_summary_json(json_str):
        results = {'result': {}}
        try:
            results = json.loads(json_str)

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error("Invalid json document {}".format(details))

        return results.get('result')

    @staticmethod
    def _parse_gemini_summary(lines):
        results = {}
        enable_parse = False

        for line in lines:
            line.strip()
            if 'Results:' in line:
                enable_parse = True
                continue
            if "run completed" in line:
                enable_parse = False
                continue
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]
            results[key] = int(value)
        return results
