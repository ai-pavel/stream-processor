FROM sbtscala/scala-sbt:eclipse-temurin-17_1.x AS build
WORKDIR /app
COPY project/ project/
COPY build.sbt ./
RUN sbt update
COPY src/ src/
RUN sbt compile

FROM sbtscala/scala-sbt:eclipse-temurin-17_1.x
WORKDIR /app
COPY --from=build /app/ ./
EXPOSE 8080
CMD ["sbt", "runMain stream.runServer"]
