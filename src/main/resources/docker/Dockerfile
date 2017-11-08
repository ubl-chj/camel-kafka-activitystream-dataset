FROM pandorasystems/karaf:${KARAF_VERSION}

MAINTAINER Christopher Johnson <christopher_hanna.johnson@uni-leipzig.de>
LABEL description = "Provides a Karaf container configured with Camel-Kafka features"

ENV MAVEN_REPO=/build/repository
ENV KARAF_RUNTIME=${KARAF_RUNTIME}
COPY ${mavenInstallPath}*/*/*SNAPSHOT.jar ${KARAF_DEPLOY}
COPY ${mavenInstallPath}*SNAPSHOT.jar ${KARAF_DEPLOY}

RUN mkdir -p ${mavenRepo}
ADD repository/ ${mavenRepo}

WORKDIR /opt/karaf/apache-karaf-${KARAF_VERSION}
COPY ${mavenInstallPath}bundles.yml .

RUN bin/start && \
    bin/client -r 10 -d 5  "feature:repo-add camel ${camelVersion}" && \
    bin/client -r 10 -d 5  "feature:install camel" && \
    cat bundles.yml | bin/client -b && \
    bin/stop

COPY ${mavenInstallPath}*.cfg etc/