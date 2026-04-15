pipeline {
    agent any

    stages {

        stage('Checkout') {
            steps {
                echo 'Cloning the repository...'
                git branch: 'main',
                    url: 'https://github.com/Techcognize-Inc/Intraday-Liquidity-Monitoring-Pipeline.git'
                echo 'Repository cloned successfully.'
            }
        }

        stage('Start Services') {
            steps {
                echo 'Stopping any running containers...'
                sh 'docker-compose down'
                echo 'Starting all Docker services...'
                sh 'docker-compose up -d'
                echo '''
==============================================================
  ALL SERVICES ARE UP AND RUNNING
==============================================================
  Airflow UI     -->  http://localhost:8085   (admin / admin)
  Flink UI       -->  http://localhost:18080
  Grafana        -->  http://localhost:13000  (admin / admin)
  Prometheus     -->  http://localhost:9090
==============================================================
  Go to Airflow at http://localhost:8085 and trigger the DAG.
==============================================================
                '''
            }
        }

    }
}
