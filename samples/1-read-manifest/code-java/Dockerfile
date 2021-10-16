FROM openjdk:8-jdk-alpine
ARG JAR_FILE=target/read-manifest-1.0-SNAPSHOT-jar-with-dependencies.jar
COPY ${JAR_FILE} comatrix-cdmloader.jar
ADD CDM.tar.gz /
ADD wait-for-it.sh /
ENTRYPOINT ["java","-jar","/comatrix-cdmloader.jar"]