@echo off
cd /d "%~dp0"
if not exist exports mkdir exports
call mvn clean package -DskipTests
java -jar data-manager-server\target\data-manager-server-1.0.0.jar
