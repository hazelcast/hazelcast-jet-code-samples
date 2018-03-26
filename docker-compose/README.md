# Hazelcast Jet Docker Compose Code Sample

In this code sample we will show that you can easily deploy Hazelcast Jet cluster inside Docker environment with Docker Compose.

The Docker Compose file (`hazelcast.yml`) contains two services `hazelcast-jet` and `hazelcast-jet-submit` respectively. Those services are using both official Hazelcast Jet Docker Images from Docker Hub.

- `hazelcast-jet` - This image starts plain Hazelcast Jet Member.
- `hazelcast-jet-submit` - This image starts the Hazelcast Jet Bootstrap application which submits a Jet computation job that was packaged in a self-contained JAR file to the Hazelcast Jet Cluster via Hazelcast Jet Client.

We will use `CoGroup` code sample to show the details of packaging it into fat-jar and submitting it to the Hazelcast Jet Cluster. More information about the `CoGroup` code sample can be found on [Co-Group Transform](../batch/co-group/src/main/java/CoGroup.java).

## Directory Level Structure
At the highest level, the source code is organized into following directories

|         Directory         | Description                                                                                                                                                                     |
|:-------------------------:|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|            `./`           | Root directory contains readme file (`README.md`), Docker Compose file (`hazelcast.yml`) and Makefile script (`Makefile`).                                     |
|          `jars/`          | When the project is built, the fat-jar containing the code sample will reside in this directory. This directory is also mount to the `hazelcast-jet-submit` container to submit |
|      `src/main/java`      | The main class (`CoGroup`) with the main method resides in this package.                                                                                                        |
| `src/main/java/datamodel` | The domain model classes resides in this package.                                                                                                                               |


# Prerequisites

- Docker with Docker Compose: [Installation guide](https://docs.docker.com/install/)

Docker service/daemon must be running for this application to work.


# Building the Application

To build and package the application, run:

```bash
mvn clean package
```

# Running the Application

After building the application, run the application with:

```bash
make up
```

This will create a one node Hazelcast Jet Cluster and a Hazelcast Jet Bootstrap Application to submit the job to the cluster.

To scale up the Hazelcast Jet Cluster to five nodes, run the following:

```bash
make scale
```

You can check server logs to see them scaled. After scaling up your cluster you might want to re-submit the job to the cluster, to do that run the following:

```bash
make restart
```

# Viewing the Logs

After running the application, see the logs with:

For Hazelcast Jet Cluster

```
make logServer
```

For Hazelast Jet Bootstrap Application

```
make logClient
```

# Killing the Application

After you are done with the cluster, you can kill it with:

```bash
make down
```





