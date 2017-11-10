FROM openjdk:8-jre

MAINTAINER Christopher Johnson <christopher_hanna.johnson@uni-leipzig.de>
LABEL description = "Provides a Kafka Stream Dataset"

ENV DATASET_RUNTIME /opt

WORKDIR ${DATASET_RUNTIME}
COPY *.tar .
RUN tar -xvf *.tar && \
mv activitystream-dataset-0.1.0-SNAPSHOT activitystream-dataset && \
chmod +x ${DATASET_RUNTIME}/activitystream-dataset/bin/activitystream-dataset
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]