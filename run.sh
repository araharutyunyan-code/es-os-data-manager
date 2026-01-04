#!/bin/bash
cd "$(dirname "$0")"
mkdir -p exports
mvn clean package -DskipTests
java -jar data-manager-server/target/data-manager-server-1.0.0.jar
