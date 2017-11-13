FROM openjdk:8-jre

MAINTAINER Christopher Johnson <christopher_hanna.johnson@uni-leipzig.de>
LABEL description = "Provides a Kafka Stream Dataset"

ENV DATASET_RUNTIME /opt

WORKDIR ${DATASET_RUNTIME}
COPY *.tar .
RUN tar -xvf *.tar && \
mv activitystream-dataset-${version} activitystream-dataset && \
chmod +x ${DATASET_RUNTIME}/activitystream-dataset/bin/activitystream-dataset
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]