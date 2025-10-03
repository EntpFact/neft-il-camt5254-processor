FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:1.21-1.1739376167
COPY build/libs/neft-il-camt5254-processor-0.0.1-SNAPSHOT.jar ../../app.jar
ENTRYPOINT ["java","-jar", "/app.jar"]