#!groovy

def call(String sct_latest_dir) {
    def sct_runner_ip =  sh(returnStdout: true, script:'cat sct_runner_ip||echo ""').trim()

    sh """#!/bin/bash
        set -xe

        eval \$(ssh-agent)
        function clean_ssh_agent {
            echo "Cleaning SSH agent"
            eval \$(ssh-agent -k)
        }
        trap clean_ssh_agent EXIT

        # still need to sync scylla-test, until replacing keys in jenkins
        ssh-add ~/.ssh/scylla-qa-ec2
        ssh-add ~/.ssh/scylla-test
        ssh-add ~/.ssh/scylla_test_id_ed25519

        if [[ ! -z "${sct_runner_ip}" ]] ; then
            rsync -L -ar -e "ssh -o StrictHostKeyChecking=no" --delete ubuntu@${sct_runner_ip}:/home/ubuntu/sct-results ${sct_latest_dir}
        else
            echo "SCT runner IP file is empty. Probably SCT Runner was not created."
            exit 1
        fi
    """
}
