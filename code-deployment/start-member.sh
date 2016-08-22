#!/bin/sh
java -cp "target/lib/*" -Dhazelcast.local.localAddress=127.0.0.1 com.hazelcast.console.ConsoleApp
