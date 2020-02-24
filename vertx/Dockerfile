# Copyright (c) 2020 Robert Sahlin
# Use of this software is governed by the Business Source License included in the LICENSE file.

FROM openjdk:8-jre-slim
ARG BACKUP_TOPIC=backup
ARG HOST
ARG HOST_PORT=443
ARG HOST_URI
ARG FREQUENCY=1000

ENV VERTICLE_FILE collector.vertx-fat.jar
ENV BACKUP_TOPIC=$BACKUP_TOPIC
ENV HOST=$HOST
ENV HOST_PORT=$HOST_PORT
ENV HOST_URI=$HOST_URI
ENV FREQUENCY=$FREQUENCY

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

EXPOSE 8080

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar $VERTICLE_FILE -DBACKUP_TOPIC=$BACKUP_TOPIC -DHOST=$HOST -DHOST_PORT=$HOST_PORT -DHOST_URI=$HOST_URI -DFREQUENCY=$FREQUENCY"]