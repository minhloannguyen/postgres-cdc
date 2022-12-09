FROM flink:1.15.2-java11

ARG FLINK_HOME=/opt/flink

RUN mkdir -p $FLINK_HOME/usrlib/flink-web-upload
RUN chmod 777 $FLINK_HOME/usrlib/flink-web-upload

ADD --chown=flink https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.15.2/flink-connector-kafka-1.15.2.jar ./lib
ADD --chown=flink https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.15.2/flink-connector-base-1.15.2.jar ./lib
ADD --chown=flink https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar ./lib
