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

import os
import logging
import itertools
import unittest
from collections import namedtuple

from sdcm import sct_config
from sdcm.utils.operator.miltitenant_common import set_stress_command_to_tenant

RPM_URL = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/' \
          '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1' \
          '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla.repo'


class ConfigurationTests(unittest.TestCase):  # pylint: disable=too-many-public-methods
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('anyconfig').setLevel(logging.ERROR)

        cls.setup_default_env()

        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        cls.conf = sct_config.SCTConfiguration()

        cls.clear_sct_env_variables()

        # some of the tests assume this basic case is define, to avoid putting this again and again in each test
        # and so we can run those tests specifically
        cls.setup_default_env()

    def tearDown(self):
        self.clear_sct_env_variables()
        self.setup_default_env()

    @classmethod
    def setup_default_env(cls):
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

    @classmethod
    def clear_sct_env_variables(cls):
        for k in os.environ:
            if k.startswith('SCT_'):
                del os.environ[k]

    def test_01_dump_config(self):
        logging.debug(self.conf.dump_config())

    def test_02_verify_config(self):
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'

        self.conf.verify_configuration()
        self.conf.check_required_files()

    def test_03_dump_help_config_yaml(self):
        logging.debug(self.conf.dump_help_config_yaml())

    def test_03_dump_help_config_markdown(self):  # pylint: disable=invalid-name
        logging.debug(self.conf.dump_help_config_markdown())

    def test_04_check_env_parse(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = '["eu-west-1", "us-east-1"]'
        os.environ['SCT_N_DB_NODES'] = '2 2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb ami-eae4f795'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('ami_id_db_scylla'), 'ami-06f919eb ami-eae4f795')

    def test_05_docker(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        self.assertIn('docker_image', conf.dump_config())
        self.assertEqual(conf.get('docker_image'), 'scylladb/scylla')

    @staticmethod
    def test_06a_docker_latest_no_loader():
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_VERSION'] = 'latest'
        os.environ['SCT_N_LOADERS'] = "0"
        docker_tag_after_processing = "fake_specific_docker_tag"

        with unittest.mock.patch.object(
                sct_config, 'get_specific_tag_of_docker_image',
                return_value=docker_tag_after_processing):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        assert conf['scylla_version'] != 'latest'

    @staticmethod
    def test_06b_docker_development():
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_VERSION'] = '666.development-blah'
        os.environ['SCT_SCYLLA_REPO_LOADER'] = RPM_URL

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    def test_07_baremetal_exception(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
        conf = sct_config.SCTConfiguration()
        self.assertRaises(AssertionError, conf.verify_configuration)

    def test_08_baremetal(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
        os.environ['SCT_DB_NODES_PRIVATE_IP'] = '["1.2.3.4", "1.2.3.5"]'
        os.environ['SCT_DB_NODES_PUBLIC_IP'] = '["1.2.3.4", "1.2.3.5"]'
        os.environ['SCT_USE_PREINSTALLED_SCYLLA'] = 'true'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertIn('db_nodes_private_ip', conf.dump_config())
        self.assertEqual(conf.get('db_nodes_private_ip'), ["1.2.3.4", "1.2.3.5"])

    def test_09_unknown_configure(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/unknown_param_in_config.yaml'
        conf = sct_config.SCTConfiguration()
        self.assertRaises(ValueError, conf.verify_configuration)

    def test_09_unknown_env(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/unknown_param_in_config.yaml'
        os.environ['SCT_WHAT_IS_THAT'] = 'just_made_this_up'
        os.environ['SCT_WHAT_IS_THAT_2'] = 'what is this ?'
        conf = sct_config.SCTConfiguration()
        with self.assertRaises(ValueError) as context:
            conf.verify_configuration()

        self.assertIn('SCT_WHAT_IS_THAT_2=what is this ?', str(context.exception))
        self.assertIn('SCT_WHAT_IS_THAT=just_made_this_up', str(context.exception))

    def test_10_longevity(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/complex_test_case_with_version.yaml'
        os.environ['SCT_AMI_ID_DB_SCYLLA_DESC'] = 'master'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('user_prefix'), 'longevity-50gb-4d-not-jenkins-maste')

    @staticmethod
    def test_10_mananger_regression():
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb ami-eae4f795'

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    def test_12_scylla_version_ami(self):
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf.get('ami_id_db_scylla'), 'ami-05f62259c7715e1a6 ami-027c1337dcb46da50')

    @staticmethod
    def test_12_scylla_version_ami_case1():  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '4.4.3'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb ami-eae4f795'

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    def test_12_scylla_version_ami_case2(self):  # pylint: disable=invalid-name
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        self.assertRaisesRegex(
            ValueError, "AMIs for scylla_version='99.0.3' not found in eu-west-1", sct_config.SCTConfiguration)

    @staticmethod
    def test_12_scylla_version_repo():
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '4.4.3'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @staticmethod
    def test_12_scylla_version_repo_case1():  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '4.4.3'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    def test_12_scylla_version_repo_case2(self):  # pylint: disable=invalid-name
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'

        self.assertRaisesRegex(ValueError, r"repo for scylla version 99.0.3 wasn't found", sct_config.SCTConfiguration)

    def test_12_scylla_version_repo_ubuntu(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        os.environ['SCT_GCE_IMAGE_DB'] = 'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7'

        expected_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list'
        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value="4.7.dev", clear=True), \
                unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=expected_repo, clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertEqual(conf.get('scylla_repo'),
                         expected_repo)
        self.assertEqual(conf.get('scylla_repo_loader'),
                         "https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-4.6.repo")

    def test_12_scylla_version_repo_ubuntu_loader_centos(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'centos'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        os.environ['SCT_GCE_IMAGE_DB'] = 'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7'

        expected_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list'
        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value="4.7.dev", clear=True), \
                unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=expected_repo, clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertEqual(conf.get('scylla_repo'),
                         expected_repo)
        self.assertEqual(conf.get('scylla_repo_loader'),
                         "https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-4.6.repo")

    def test_12_k8s_scylla_version_ubuntu_loader_centos(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'k8s-local-kind'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'centos'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertFalse(conf.get('scylla_repo'))
        self.assertEqual(conf.get('scylla_repo_loader'),
                         'https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-4.6.repo')

    def test_13_scylla_version_ami_branch(self):  # pylint: disable=invalid-name
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = 'branch-5.0:32'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf.get('ami_id_db_scylla'), 'ami-076a213c791dc19cd ami-0625f2d18e05bf206')

    def test_13_scylla_version_ami_branch_latest(self):  # pylint: disable=invalid-name
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = 'branch-4.2:latest'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertIsNotNone(conf.get('ami_id_db_scylla'))
        self.assertEqual(len(conf.get('ami_id_db_scylla').split(' ')), 2)

    def test_conf_check_required_files(self):  # pylint: disable=no-self-use
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ['SCT_STRESS_CMD'] = """cassandra-stress user profile=/tmp/cs_profile_background_reads_overload.yaml \
            ops'(insert=100)' no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore"""
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.check_required_files()

    def test_conf_check_required_files_negative(self):  # pylint: disable=no-self-use
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/stress_cmd_with_bad_profile.yaml'
        os.environ['SCT_STRESS_CMD'] = """cassandra-stress user profile=/tmp/1232123123123123123.yaml ops'(insert=100)'\
            no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore"""
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        try:
            conf.check_required_files()
        except ValueError as exc:
            self.assertEqual(str(
                exc), "Stress command parameter \'stress_cmd\' contains profile \'/tmp/1232123123123123123.yaml\' that"
                      " does not exists under data_dir/")

    def test_config_dupes(self):
        def get_dupes(c):
            '''sort/tee/izip'''

            # pylint: disable=invalid-name
            a, b = itertools.tee(sorted(c))
            next(b, None)
            r = None
            for k, g in zip(a, b):
                if k != g:
                    continue
                if k != r:
                    yield k
                    r = k

        opts = [o['name'] for o in sct_config.SCTConfiguration.config_options]

        self.assertListEqual(list(get_dupes(opts)), [])

    def test_13_bool(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_STORE_PERF_RESULTS'] = 'False'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()

        self.assertEqual(conf['store_perf_results'], False)

    def test_14_aws_siren_from_env(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_DB_TYPE'] = 'cloud_scylla'
        os.environ['SCT_REGION_NAME'] = 'us-east-1'
        os.environ['SCT_N_DB_NODES'] = '2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
        os.environ['SCT_AUTHENTICATOR_USER'] = "user"
        os.environ['SCT_AUTHENTICATOR_PASSWORD'] = "pass"
        os.environ['SCT_CLOUD_CLUSTER_ID'] = "193712947904378"
        os.environ['SCT_SECURITY_GROUP_IDS'] = "sg-89fi3rkl"
        os.environ['SCT_SUBNET_ID'] = "sub-d32f09sdf"

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('cloud_cluster_id'), 193712947904378)
        assert conf["authenticator_user"] == "user"
        assert conf["authenticator_password"] == "pass"
        assert conf["security_group_ids"] == ["sg-89fi3rkl"]
        assert conf["subnet_id"] == ["sub-d32f09sdf"]

    def test_15_new_scylla_repo(self):
        centos_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/' \
                      '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1' \
                      '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla' \
                      '.repo'

        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_REPO'] = centos_repo
        os.environ['SCT_NEW_SCYLLA_REPO'] = centos_repo
        os.environ['SCT_USER_PREFIX'] = 'testing'

        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value='2019.1.1', clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()
            conf._get_target_upgrade_version()  # pylint: disable=protected-access
        self.assertEqual(conf.get('target_upgrade_version'), '2019.1.1')

    def test_15a_new_scylla_repo_by_scylla_version(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        os.environ['SCT_NEW_VERSION'] = 'master:latest'
        os.environ['SCT_USER_PREFIX'] = 'testing'
        os.environ['SCT_GCE_IMAGE_DB'] = \
            'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7'

        resolved_repo_link = 'https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/rpm\
            /centos/2021-06-09T13:12:44Z/scylla.repo'

        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value='666.development', clear=True), \
                unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=resolved_repo_link, clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()
            conf._get_target_upgrade_version()  # pylint: disable=protected-access

        self.assertEqual(conf.get('scylla_repo'), resolved_repo_link)
        target_upgrade_version = conf.get('target_upgrade_version')
        self.assertTrue(target_upgrade_version == '666.development' or target_upgrade_version.endswith(".dev"))

    def test_15b_image_id_by_scylla_version(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        os.environ['SCT_USER_PREFIX'] = 'testing'
        os.environ['SCT_GCE_IMAGE_DB'] = ''
        os.environ['SCT_USE_PREINSTALLED_SCYLLA'] = 'true'

        resolved_image_link = 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/' \
                              'scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359'
        image = namedtuple("Image", "name extra")(name='scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359',
                                                  extra=dict(selfLink=resolved_image_link))

        with unittest.mock.patch.object(sct_config, 'get_branched_gce_images', return_value=[image], clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()
            conf._get_target_upgrade_version()  # pylint: disable=protected-access

        self.assertEqual(conf.get('gce_image_db'), resolved_image_link)

    def test_16_default_oracle_scylla_version_eu_west_1(self):
        ami_4_4_7 = "ami-0cac6b91be579df80"

        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf.get('ami_id_db_oracle'), ami_4_4_7)

    def test_16_oracle_scylla_version_us_east_1(self):
        ami_4_5_2 = "ami-0e075abbcb95ac10a"

        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = "4.5.2"
        os.environ['SCT_REGION_NAME'] = 'us-east-1'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-eae4f795'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf.get('ami_id_db_oracle'), ami_4_5_2)

    def test_16_oracle_scylla_version_eu_west_1(self):
        ami_4_5_2 = "ami-057f1cfd8877782b2"

        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = "4.5.2"
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf.get('ami_id_db_oracle'), ami_4_5_2)

    def test_16_oracle_scylla_version_wrong_region(self):
        ami_4_5_2_eu_west_1 = "ami-057f1cfd8877782b2"

        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = "4.5.2"
        os.environ['SCT_REGION_NAME'] = 'us-east-1'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-eae4f795'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertNotEqual(conf.get('ami_id_db_oracle'), ami_4_5_2_eu_west_1)

    def test_16_oracle_scylla_version_and_oracle_ami_together(self):

        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = '3.0.11'
        os.environ['SCT_AMI_ID_DB_ORACLE'] = 'ami-0535c88b85b914499'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

        with self.assertRaises(ValueError) as context:
            sct_config.SCTConfiguration()

        self.assertIn("'oracle_scylla_version' and 'ami_id_db_oracle' can't used together", str(context.exception))

    def test_17_verify_scylla_bench_required_parameters_in_command(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_STRESS_CMD'] = "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
        os.environ["SCT_STRESS_READ_CMD"] = "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf["stress_cmd"], [os.environ['SCT_STRESS_CMD']])
        self.assertEqual(conf["stress_read_cmd"], [os.environ["SCT_STRESS_READ_CMD"]])

    def test_17_1_raise_error_if_scylla_bench_command_dont_have_workload(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_STRESS_CMD'] = "scylla-bench -mode=write -replication-factor=3 -partition-count=100"
        os.environ["SCT_STRESS_READ_CMD"] = "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"

        err_msg = f"Scylla-bench command {os.environ['SCT_STRESS_CMD']} doesn't have parameter -workload"

        with self.assertRaises(ValueError) as context:
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn(err_msg, str(context.exception))

    def test_17_2_raise_error_if_scylla_bench_command_dont_have_mode(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_STRESS_CMD'] = "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
        os.environ["SCT_STRESS_READ_CMD"] = "scylla-bench -workload=uniform -replication-factor=3 -partition-count=100"

        err_msg = f"Scylla-bench command {os.environ['SCT_STRESS_READ_CMD']} doesn't have parameter -mode"

        with self.assertRaises(ValueError) as context:
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn(err_msg, str(context.exception))

    def test_18_error_if_no_version_repo_ami_selected(self):
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)

        for backend in sct_config.SCTConfiguration.available_backends:
            if 'k8s' in backend:
                continue
            if 'siren' in backend:
                continue

            os.environ['SCT_CLUSTER_BACKEND'] = backend
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

            conf = sct_config.SCTConfiguration()
            with self.assertRaises(AssertionError, msg=f"{backend} didn't failed") as context:
                conf.verify_configuration()
            self.assertIn("scylla version/repos wasn't configured correctly", str(context.exception), )

            self.clear_sct_env_variables()

    def test_18_error_if_no_version_repo_ami_selected_siren(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_DB_TYPE'] = 'cloud_scylla'

        # 1) check that SCT_CLOUD_CLUSTER_ID is used for the exception
        conf = sct_config.SCTConfiguration()

        with self.assertRaises(AssertionError, msg="cloud_scylla didn't failed") as context:
            conf.verify_configuration()
        self.assertIn("scylla version/repos wasn't configured correctly", str(context.exception))
        self.assertIn("SCT_CLOUD_CLUSTER_ID", str(context.exception))

        # 2) check that when SCT_CLOUD_CLUSTER_ID is defined, the verification works
        os.environ['SCT_CLOUD_CLUSTER_ID'] = '1234'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    def test_19_aws_region_with_no_region_data(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'not-exists-2'

        with self.assertRaisesRegex(ValueError, expected_regex="not-exists-2 isn't supported"):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

    def test_20_user_data_format_version_aws(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = '4.6.0'
        os.environ['SCT_DB_TYPE'] = 'mixed_scylla'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '3')
        self.assertEqual(conf['oracle_user_data_format_version'], '2')

    def test_20_user_data_format_version_aws_2(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-07d54ca4e98347364'  # run image which isn't scylla
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertNotIn('user_data_format_version', conf)

    def test_20_user_data_format_version_gce_1(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '3')

    def test_20_user_data_format_version_gce_2(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_GCE_IMAGE_DB'] = 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-4-6-4'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '2')

    def test_20_user_data_format_version_gce_3(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_GCE_IMAGE_DB'] = ('https://www.googleapis.com/compute/v1/projects/'
                                          'ubuntu-os-cloud/global/images/family/ubuntu-2004-lts')
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '2')

    def test_20_user_data_format_version_azure(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'azure'
        os.environ['SCT_AZURE_REGION_NAME'] = 'eastus'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        # since azure image listing is still buggy, can be sure which version we'll get
        # I would expect master:latest to be version 3 now, but azure.utils.get_scylla_images
        # returns something from 5 months ago.
        self.assertIn('user_data_format_version', conf)

    def test_21_verify_unique_stress_cmds_per_tenant(self):
        os.environ['SCT_CONFIG_FILES'] = 'unit_tests/test_data/test_config/multitenant/unique_stress_cmd_per_tenant.yaml'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        tenant_stress_cmds = set_stress_command_to_tenant(conf, 0)
        self.assertEqual(tenant_stress_cmds['stress_cmd'], ['threads=50', 'threads=30'])
        self.assertEqual(tenant_stress_cmds['stress_read_cmd'], ['threads=70'])
        self.assertEqual(tenant_stress_cmds['prepare_write_cmd'], ['threads=90'])

        tenant_stress_cmds = set_stress_command_to_tenant(conf, 1)
        self.assertEqual(tenant_stress_cmds['stress_cmd'], ['threads=60', 'threads=20'])
        self.assertEqual(tenant_stress_cmds['stress_read_cmd'], ['threads=10'])
        self.assertEqual(tenant_stress_cmds['prepare_write_cmd'], ['threads=70'])

    def test_22_verify_same_stress_cmds_for_all_tenants(self):
        os.environ['SCT_CONFIG_FILES'] = 'unit_tests/test_data/test_config/multitenant/same_stress_cmd_for_all_tenants.yaml'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        tenant_stress_cmds_0 = set_stress_command_to_tenant(conf, 0)
        self.assertEqual(tenant_stress_cmds_0['stress_cmd'], ["threads=50", "threads=30"])
        self.assertEqual(tenant_stress_cmds_0['stress_read_cmd'], ["threads=70", "threads=10"])
        self.assertEqual(tenant_stress_cmds_0['prepare_write_cmd'], ["threads=90", "threads=70"])

        tenant_stress_cmds_1 = set_stress_command_to_tenant(conf, 1)
        self.assertEqual(tenant_stress_cmds_0['stress_cmd'], tenant_stress_cmds_1['stress_cmd'])
        self.assertEqual(tenant_stress_cmds_0['stress_read_cmd'], tenant_stress_cmds_1['stress_read_cmd'])
        self.assertEqual(tenant_stress_cmds_0['prepare_write_cmd'], tenant_stress_cmds_1['prepare_write_cmd'])

    def test_23_verify_mixed_stress_cmds_for_tenants(self):
        os.environ['SCT_CONFIG_FILES'] = 'unit_tests/test_data/test_config/multitenant/mixed_stress_cmd_per_tenant.yaml'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        tenant_stress_cmds_0 = set_stress_command_to_tenant(conf, 0)
        self.assertEqual(tenant_stress_cmds_0['stress_cmd'], ['threads=50', 'threads=30'])
        self.assertEqual(tenant_stress_cmds_0['stress_read_cmd'], ['threads=70'])
        self.assertEqual(tenant_stress_cmds_0['prepare_write_cmd'], ["threads=90", "threads=70"])

        tenant_stress_cmds_1 = set_stress_command_to_tenant(conf, 1)
        self.assertEqual(tenant_stress_cmds_1['stress_cmd'], ['threads=60', 'threads=20'])
        self.assertEqual(tenant_stress_cmds_1['stress_read_cmd'], ['threads=10'])
        self.assertEqual(tenant_stress_cmds_0['prepare_write_cmd'], tenant_stress_cmds_1['prepare_write_cmd'])


if __name__ == "__main__":
    unittest.main()
