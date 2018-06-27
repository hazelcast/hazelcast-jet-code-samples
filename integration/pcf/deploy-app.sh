#!/bin/sh

mvn clean compile install -DskipTests
cf push
