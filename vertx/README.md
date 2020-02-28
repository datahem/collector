# collector
Collect data sent over HTTP from trackers and publish the data on pubsub.

*package
mvn clean package
mvn package jib:build -Djib.container.environment=HOST="datahem-vkd3zhb3jq-lz.a.run.app",HOST_PORT="443",HOST_URI="/topic/tmp",FREQUENCY="2"
mvn package jib:build -Djib.container.environment=HOST="datahem-vkd3zhb3jq-lz.a.run.app",HOST_PORT=443,HOST_URI="/topic/tmp",FREQUENCY=2,BACKUP_TOPIC="backup",ALLOWED_ORIGINS_PATTERN=".*.",PACKAGE="org.meshr.collector.vertx",VERSION="0.9.0"

*run locally
java -jar target/collector.vertx-fat.jar -DHTTP_PORT=8080 -DBACKUP_TOPIC=tmp -DHOST=datahem-vkd3zhb3jq-lz.a.run.app -DHOST_PORT=80 -DHOST_URI=/optimize/default/topic/tmp -DFREQUENCY=2 -DVERSION=1.0.0

*create docker image and push to container registry
gcloud builds submit --config=cloudbuild.yaml . --substitutions=_VERSION=0.9.0

# Version

## 0.1.0 (2020-02-10): Cloud Run Vert.x
Initial Cloud Run Vert.x collector with eventbus and asyncronous PublisherVerticle.
Added license.
Added config options for setting backup topic.
Added modes for reliable or optimistic collection to optimize latency.