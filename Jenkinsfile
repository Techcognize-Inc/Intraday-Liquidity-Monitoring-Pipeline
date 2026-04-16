pipeline {
    agent any

    stages {

        stage('Checkout') {
            steps {
                cleanWs()
                echo 'Cloning the repository...'
                git branch: 'main',
                    url: 'https://github.com/Techcognize-Inc/Intraday-Liquidity-Monitoring-Pipeline.git'
                echo 'Repository cloned successfully.'
            }
        }

        stage('Verify Services') {
            steps {
                echo 'Checking all pipeline services are running...'
                sh '''
                    for svc in zookeeper kafka schema-registry postgres flink-jobmanager flink-taskmanager airflow-webserver airflow-scheduler payment-producer prometheus grafana; do
                        STATUS=$(docker inspect --format="{{.State.Status}}" $svc 2>/dev/null || echo "missing")
                        echo "$svc --> $STATUS"
                        if [ "$STATUS" != "running" ]; then
                            echo "ERROR: $svc is not running!"
                            exit 1
                        fi
                    done
                '''
                echo '''
==============================================================
  ALL SERVICES ARE UP AND RUNNING
==============================================================
  Airflow UI     -->  http://localhost:8085   (admin / admin)
  Flink UI       -->  http://localhost:18080
  Grafana        -->  http://localhost:13000  (admin / admin)
  Prometheus     -->  http://localhost:9090
==============================================================
                '''
            }
        }

    }
}
