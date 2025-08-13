#!groovy

def call() {

    def builder = getJenkinsLabels("aws", "eu-west-1")

    pipeline {
        agent {
            label {
                   label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }
        parameters {
            string(defaultValue: "",
               description: 'folder name or path in jenkins jobs structure',
               name: 'jenkins_path')
            string(defaultValue: "master",
               description: 'sct branch',
               name: 'sct_branch')
            string(defaultValue: "git@github.com:scylladb/scylla-cluster-tests.git",
               description: 'sct repo link',
               name: 'sct_repo')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
//         triggers {
//             parameterizedCron (
//                 '''
//                     H 01 * * 0 %jenkins_path="scylla-master/releng-testing"
//                     H 01 * * 0 %jenkins_path="scylla-master"
//                     H 01 * * 0 %sct_branch=branch-perf-v15
//                     H 01 * * 0 %sct_branch=branch-perf-v16
//                     H 01 * * 0 %sct_branch=branch-perf-v17
//                 '''
//             )
//         }
        stages {
            stage('Checkout sct') {
                steps {
                    script {
                        completed_stages = [:]
                    }
                    dir('scylla-cluster-tests') {
                        timeout(time: 5, unit: 'MINUTES') {
                            git(url: params.sct_repo,
                                credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
                                branch: params.sct_branch)
                        }
                    }
                }
            }
            stage('Create test jobs') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        timeout(time: 360, unit: 'MINUTES') {
                            withCredentials([usernamePassword(credentialsId: 'jenkins-api-token', passwordVariable: 'JENKINS_PASSWORD', usernameVariable: 'JENKINS_USERNAME')]) {
                                script {
                                    wrap([$class: 'BuildUser']) {
                                        dir('scylla-cluster-tests') {
                                            sh """#!/bin/bash
                                                set -xe
                                                env
                                                if [[ -n "${params.jenkins_path}" ]]; then
                                                    echo "start create test jobs for branch ${params.jenkins_path} ......."
                                                fi
                                                if [[ "${params.jenkins_path}" == "scylla-master" ]] ; then
                                                    echo "start create operator test jobs for operator-master ......."
                                                fi
                                                if [[ "${params.jenkins_path}" == "scylla-master" ]] ; then
                                                    echo "start create qa tools jobs  ......."
                                                fi

                                                if [[ "${params.sct_branch}" == "branch-perf-v15" ]] ; then
                                                    echo "start create perf for ${params.sct_branch}  ......."
                                                fi

                                                if [[ "${params.sct_branch}" == "branch-perf-v16" ]] ; then
                                                    echo "start create perf for ${params.sct_branch}  ......."
                                                fi

                                                if [[ "${params.sct_branch}" == "branch-perf-v17" ]] ; then
                                                    echo "start create perf for ${params.sct_branch}  ......."
                                                fi

                                                """
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
