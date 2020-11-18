#FROM maven:3.5-jdk-8 as maven
#
## copy the project files
#COPY ./pom.xml ./pom.xml
#
## build all dependencies
#RUN mvn dependency:go-offline -B
#
## copy your other files
#
#
## build for release
#RUN mvn package

# our final base image
FROM openjdk:12-oraclelinux7

RUN yum -y install \
    libXi \
   libXrender

COPY ./ ./


COPY ./data ./data

RUN yum -y install libXtst

WORKDIR /cs1660-finalproject

COPY credentials.json ./

# copy over the built artifact from the maven image
COPY target/cs1660-finalproject-1.0-SNAPSHOT-jar-with-dependencies.jar ./

# set the startup command to run your binary
CMD ["java",  "-jar", "./cs1660-finalproject-1.0-SNAPSHOT-jar-with-dependencies.jar"]