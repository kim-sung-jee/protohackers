# syntax=docker/dockerfile:1
FROM gradle:8.8-jdk21 AS build
WORKDIR /workspace

COPY gradlew ./gradlew
COPY gradle   ./gradle
COPY settings.gradle.kts build.gradle.kts ./
RUN chmod +x ./gradlew

COPY src ./src
RUN ./gradlew --no-daemon clean bootJar \
 && ls -l build/libs \
 && mv build/libs/*.jar app.jar

FROM eclipse-temurin:21-jre
WORKDIR /app

RUN useradd -r -s /bin/false app
USER app

COPY --from=build /workspace/app.jar /app/app.jar

EXPOSE 7007

ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
