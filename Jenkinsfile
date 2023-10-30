#!groovy
//Jenkinsfile (Declarative Pipeline)

pipeline{
    environment {
        credentialId = 'aliregistry'
        url = "https://registry-intl.ap-southeast-5.aliyuncs.com"
        scannerHome = tool 'Sonarqube'
        servicename = 'svc-datamart'
        servicename2 = 'svc-datamart-convert-postgre'
    }
    agent any
    stages {
        stage('Publish Approval Datamart Postgre') {
            when { 
                tag "release-postgre-*"
            }
            steps {
                script{
                //   input message: "Deploy these changes?", submitter "admin"
                def userName = input message: 'Deploy these changes?', submitter: "faris,grandis", submitterParameter: "faris,grandis"
                echo "Accepted by ${userName}"
                if (!(['faris','grandis'].contains(userName))) {
                    error('This user is not approved to deploy to PROD.')
                }
                }
            }
        }
        stage('get env convert postgre Release Prod') {
            when { 
                tag 'release-postgre-*';
            }
            steps {
                withCredentials([file(credentialsId: 'envconvertpostgreprod', variable: 'envconvertpostgreprod')]) {
                    sh "cat \$envconvertpostgreprod >> ${WORKSPACE}/.env"
                }
            } 
        }
        stage('Build Image Tag Datamart Postgre Release Prod') {
            when { 
                tag "release-postgre-*"
            }
            steps {
                slackSend (color: '#FFFF00', message: "STARTED: Build Image Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                echo 'Build Image Prod'
                sh 'docker build . -t ${servicename2}:${TAG_NAME}-${BUILD_NUMBER} --build-arg SSH_PRIVATE_KEY="$(cat ../id_rsa)"'
                sh 'echo ini build image prod'
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Build Image Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Build Image Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }   
        }
        stage('Docker Login tag push Tag Datamart Postgre Release Prod') {
            when { 
                tag "release-postgre-*"
            }
            steps {
                script {
                    echo 'Push docker image ke docker registry Ali Prod'
                    docker.withRegistry(url, credentialId) {
                        sh 'docker tag ${servicename2}:${TAG_NAME}-${BUILD_NUMBER} registry-intl.ap-southeast-5.aliyuncs.com/majoo/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}'
                        sh 'docker push registry-intl.ap-southeast-5.aliyuncs.com/majoo/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}'
                        sh 'echo ini docker login tag push prod'
                    }
                }
            }
        }
        stage('Set Image Kubernetes Tag Datamart Postgre Release Prod') {
            when { 
                tag "release-postgre-*"
            }
            steps {
                script {
                    //sh 'kubectl --kubeconfig="../../k8s-prod-data-analytics-kubeconfig.yaml" set image deployment ${servicename2} ${servicename2}=registry-intl.ap-southeast-5.aliyuncs.com/majoo/${servicename2}:${TAG_NAME}-${BUILD_NUMBER} -n=prod'
                    sh 'echo set image k8s prod'
                }
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Deployment Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Deployment Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }
        }
        
        stage('Set Image VM Tag ETl Datamart Product Release Prod') {
            when { 
                tag "release-postgre-*"
            }
            steps {
                script {
                    sshagent (credentials: ['etl-datamart-product']) {
                     sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.128.15.211 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                     echo 'set image etl klopos 1'
                     sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.128.15.212 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                     echo 'set image etl klopos 2'
                     sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.128.15.213 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                     echo 'set image etl klopos 3'
                     sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.128.15.214 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                    }
                }
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Deployment Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Deployment Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }
        }
        stage('Set Image VM Tag Datamart Akunting Release Prod') {
            when { 
                tag "release-postgre-*"
            }
            steps {
                script {
                    sshagent (credentials: ['datamart-etl-akunting-prod']) {
                     sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.128.15.202 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                     sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.128.15.243 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${TAG_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                    }
                }
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Deployment Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Deployment Prod ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }
        }
        stage('get env convert postgre') {
            when { 
                branch 'etl-convert-to-postgres';
            }
            steps {
                withCredentials([file(credentialsId: 'envconvertpostgre', variable: 'envconvertpostgre')]) {
                    sh "cat \$envconvertpostgre >> ${WORKSPACE}/.env"
                }
            } 
        }
        stage('Build Image convert postgre') {
            when { 
                branch 'etl-convert-to-postgres';
            }
            steps {
                slackSend (color: '#FFFF00', message: "STARTED: Build Image Beta ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                echo 'Build Image beta'
                // sh 'docker build . -t ${servicename}:${BRANCH_NAME}-${BUILD_NUMBER} --build-arg SSH_PRIVATE_KEY="$(cat /var/lib/jenkins/id_rsa)"'
                sh 'docker build . -t ${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER} --build-arg SSH_PRIVATE_KEY="$(cat ../id_rsa)"'
                sh 'echo ini build image beta'
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Build Image Beta ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Build Image Beta ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }   
        }
        stage('Docker Login tag push convert postgre') {
            when { 
                branch 'etl-convert-to-postgres';
            }
            steps {
                script {
                    echo 'Push docker image ke docker registry Ali Beta'
                    docker.withRegistry(url, credentialId) {
                        sh 'docker tag ${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER} registry-intl.ap-southeast-5.aliyuncs.com/majoo/${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER}'
                        sh 'docker push registry-intl.ap-southeast-5.aliyuncs.com/majoo/${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER}'
                        sh 'echo ini docker login tag push Beta'
                    }
                }
            }
        }
        stage('Set Image Kubernetes convert postgre') {
            when { 
                branch 'etl-convert-to-postgres';
            }
            steps {
                script {
                    sh 'kubectl --kubeconfig="../../kubeconfig-stg-enterprise-nsetldatamart.yaml" set image deployment ${servicename2} ${servicename2}=registry-intl.ap-southeast-5.aliyuncs.com/majoo/${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER} -n=staging'
                    sh 'echo set image k8s Beta'
                }
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Deployment Beta ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Deployment Beta ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }
        }
        stage('Set VM docker-compose etl-postgres-klopos-staging') {
            when { 
                branch "etl-convert-to-postgres"
            }
            steps {
                script {
                    sshagent (credentials: ['etl-staging-gcp']) {
                    sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.132.0.21 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                    }
                }
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Deployment etl-postgres-klopos-staging ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Deployment etl-postgres-klopos-staging ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }
        }
        stage('Set VM docker-compose etl-postgres-akunting-staging') {
            when { 
                branch "etl-convert-to-postgres"
            }
            steps {
                script {
                    sshagent (credentials: ['etl-postgres-akunting-staging']) {
                    sh 'ssh -t -o StrictHostKeyChecking=no -l majoo 10.132.0.20 "cd ~/etl; pwd; ls -la; sed -i "s/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER}/g" docker-compose.yml; docker compose up -d; cat docker-compose.yml; sed -i "s/${servicename2}:${BRANCH_NAME}-${BUILD_NUMBER}/{{SERVICE_NAME}}:{{JOB_NAME}}-{{BUILD_NUMBER}}/g" docker-compose.yml; cat docker-compose.yml;"'
                     }
                }
            }
            post {
                success {
                    slackSend (color: '#008000', message: "SUCCESS: Deployment etl-postgres-akunting-staging ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
                failure {
                    slackSend (color: '#FF0000', message: "FAILED: Deployment etl-postgres-akunting-staging ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
                }
            }
        }
    }

    post {
        success {
            slackSend (color: '#008000', message: "SUCCESS: Pipeline ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
        }
        failure {
            slackSend (color: '#FF0000', message: "FAILED: Pipeline ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)")
        }
    }
}
