FROM flink:1.15.2-java11

ARG FLINK_HOME=/opt/flink

RUN mkdir -p $FLINK_HOME/usrlib/flink-web-upload
RUN chmod 777 $FLINK_HOME/usrlib/flink-web-upload
