# Copyright (c) 2020 Robert Sahlin
# Use of this software is governed by the Business Source License included in the LICENSE file.

FROM openjdk:8-jre-slim
ENV VERTICLE_FILE collector.vertx-1.0-SNAPSHOT-fat.jar
ENV BACKUP_TOPIC backup

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

EXPOSE 8080

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
#CMD ["exec java -jar $VERTICLE_FILE"]
CMD ["exec java -jar $VERTICLE_FILE -DBACKUP_TOPIC=$BACKUP_TOPIC"]