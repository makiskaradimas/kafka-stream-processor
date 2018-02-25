FROM java:openjdk-8-jre

RUN apt-get update && \
    apt-get install -y lib32stdc++6 && \
    apt-get install -y libstdc++6

VOLUME /tmp

ADD kafka-stream-processor-0.1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
RUN sh -c 'touch /app.jar'

ADD wait-for-topics.sh wait-for-topics.sh
RUN sed -i -e 's/\r$//' /wait-for-topics.sh && chmod +x /wait-for-topics.sh

ENV JAVA_OPTS=""
ENTRYPOINT [ "sh", "-c", "/wait-for-topics.sh" ]