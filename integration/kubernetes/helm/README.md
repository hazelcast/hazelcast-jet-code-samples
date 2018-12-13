## Hazelcast Jet Helm Chart Code Sample

This sample is a guideline on how to start a Hazelcast Jet cluster, submit a 
sample rolling aggregation job and inspect the cluster via Hazelcast Jet 
Management Center on the Kubernetes environment with Helm package Manager.

## Prerequisites

1. Up and running [Kubernetes](https://kubernetes.io) version 1.9 or higher.
   * You must have the Kubernetes command line tool, [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/),
    installed
2. [Helm Package Manager](https://helm.sh/)
3. Familiarity with `kubectl`, Kubernetes, and Docker.
4. Git
5. Apache Maven
6. Java 8+


## Starting Hazelcast Jet Cluster

To deploy Hazelcast Jet using Helm, the Hazelcast Helm Charts repository 
needs to be added to the repositories list using the following command:

```bash
$ helm repo add hazelcast https://hazelcast.github.io/charts/
``` 

Then, update the information of available charts locally from chart repositories 
using the following command:

```bash
$ helm repo update
``` 

Then, the chart can be installed using the following command:

```bash
$ helm install --name first-release --set image.tag=latest-snapshot,managementcenter.licenseKey=<YOUR_LICENSE_KEY> hazelcast/hazelcast-jet
```
 
IMPORTANT: See [Inspecting the Job with Hazelcast Jet Management Center](#inspecting-the-job-with-hazelcast-jet-management-center)
for more information about Hazelcast Jet Management Center and licensing.

IMPORTANT: If a release name used different than above or do not specified one 
to let Helm to create one, make sure the `hazelcast-client.xml` in 
`rolling-aggregation-via-helm.yaml` file has correct service name for discovery 
mechanism to work properly.

See [Hazelcast Jet Helm Chart](https://github.com/hazelcast/charts/tree/master/stable/hazelcast-jet)
section for more details on configuration and [Troubleshooting in Kubernetes Environments](https://github.com/hazelcast/charts#troubleshooting-in-kubernetes-environments) 
section if you encounter any issues.


## Creating Role Binding

Hazelcast Jet Client uses Kubernetes API to discover nodes and that is why you need to 
grant certain permissions. The simplest Role Binding file can look as `rbac.yaml`. 
Note that you can make it more specific, since Hazelcast Jet actually uses only 
certain API endpoints. Note also that if you use "DNS Lookup Discovery" instead 
of "REST API Discovery", then you can skip the Role Binding step at all. Read 
more at [Hazelcast Kubernetes API Plugin](https://github.com/hazelcast/hazelcast-kubernetes).

You can apply the Role Binding with the following command:

```bash
$ kubectl apply -f rbac.yaml
```

## Running a Job on Hazelcast Jet Cluster

To submit a job to a Hazelcast Jet cluster, the job needs to be packaged as a 
Docker image. Refer to the [Packaging a Job as a Docker Container](../README.md#packaging-a-job-as-a-docker-container) 
section for details.

After a Docker image has been built for the job, it can be run with the 
following command:

```bash
$ kubectl apply -f rolling-aggregation-via-helm.yaml
```

Check out the inspection sections below for more details on the job.  

## Inspecting the Job with Hazelcast Jet Management Center

Hazelcast Jet Management Center enables monitoring and managing your cluster 
members running Hazelcast Jet. In addition to monitoring the overall state of 
the clusters, it can also helps to analyze and browse the jobs in detail. https://github.com/hazelcast/charts/tree/master/stable/hazelcast-jet[Hazelcast Jet Helm Chart] 
includes a Hazelcast Jet Management Center deployment.

See [Hazelcast Jet Documentation](http://docs.hazelcast.org/docs/jet/latest/manual) 
and [Hazelcast Jet Management Center Documentation](https://docs.hazelcast.org/docs/jet-management-center/latest/manual/) 
for more information.

Hazelcast Jet Management Center requires a license key. Free trial is limited 
to a single node, you can apply for a trial license key from [here](https://hazelcast.com/hazelcast-enterprise-download/). 

### Configuring the License Key

The license key can be provided while creating a Helm release with the following 
command:

```bash
$ helm install --name first-release --set managementcenter.licenseKey=<YOUR_LICENSE_KEY> hazelcast/hazelcast-jet
```

Alternatively, the license key can be stored as a Kubernetes Secret with the 
following command:

```bash
$ kubectl create secret generic <NAME_OF_THE_SECRET> --from-literal=key=<YOUR_LICENSE_KEY>
```

Then, the name of the secret provided to the Helm release with the following 
command:
     
```bash
$ helm install --name first-release --set managementcenter.licenseKeySecretName=<NAME_OF_THE_SECRET> hazelcast/hazelcast-jet
```

### Retrieving the Hazelcast Jet Management Center URL

After a successful Helm release, the Hazelcast Jet Management Center URL can be 
retrieved by following the instructions on the end of the Helm release logs.

```bash
To access Hazelcast Jet Management Center:
  *) Check Management Center external IP:
     $ export MANAGEMENT_CENTER_IP=$(kubectl get svc --namespace default first-release-hazelcast-jet-management-center -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
  *) Open Browser at: http://$MANAGEMENT_CENTER_IP:8081/
```

## Inspecting the Hazelcast Jet Logs

After successful submission of the job, it's logs can be inspected with the 
following commands below:

```bash
$ export POD_NAME=$(kubectl get pods --selector=job-name=rolling-aggregation  -o=jsonpath='{.items[0].metadata.name}')
$ kubectl logs $POD_NAME
```

The command above should output like below:

```
Picked up JAVA_TOOL_OPTIONS: -Dhazelcast.client.config=/config/hazelcast-jet/hazelcast-client.xml
12:50,009 INFO impl.config.XmlJetConfigLocator Loading configuration /config/hazelcast-jet/hazelcast-client.xml from property hazelcast.client.config
12:50,011 INFO impl.config.XmlJetConfigLocator Using configuration file at /config/hazelcast-jet/hazelcast-client.xml
12:50,368 INFO hazelcast.client.HazelcastClient hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] A non-empty group password is configured for the Hazelcast client. Starting with Hazelcast version 3.11, clients with the same group name, but with different group passwords (that do not use authentication) will be accepted to a cluster. The group password configuration will be removed completely in a future release.
12:50,410 INFO hazelcast.core.LifecycleService hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] HazelcastClient 3.11 (20181023 - 1500bbb) is STARTING
12:50,911 INFO discovery.integration.DiscoveryService hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Kubernetes Discovery properties: { service-dns: null, service-dns-timeout: 5, service-name: first-release-hazelcast-jet, service-port: 0, service-label: null, service-label-value: true, namespace: default, resolve-not-ready-addresses: false, kubernetes-master: https://kubernetes.default.svc}
12:50,917 INFO discovery.integration.DiscoveryService hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Kubernetes Discovery activated resolver: ServiceEndpointResolver
12:50,949 INFO client.spi.ClientInvocationService hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Running with 2 response threads
12:51,058 INFO hazelcast.core.LifecycleService hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] HazelcastClient 3.11 (20181023 - 1500bbb) is STARTED
12:51,799 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Trying to connect to [10.0.6.10]:5701 as owner member
12:51,862 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Setting ClientConnection{alive=true, connectionId=1, channel=NioChannel{/10.0.4.6:35350->/10.0.6.10:5701}, remoteEndpoint=[10.0.6.10]:5701, lastReadTime=2018-12-03 11:12:51.858, lastWriteTime=2018-12-03 11:12:51.855, closedTime=never, connected server version=3.11} as owner with principal ClientPrincipal{uuid='99593179-6b6e-4fa4-8daa-06608a2c8ab7', ownerUuid='83763665-221e-4d67-b34e-465b2c96893b'}
12:51,863 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Authenticated with server [10.0.6.10]:5701, server version:3.11 Local address: /10.0.4.6:35350
12:51,874 INFO spi.impl.ClientMembershipListener hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] 

Members [2] {
	Member [10.0.6.10]:5701 - 83763665-221e-4d67-b34e-465b2c96893b
	Member [10.0.6.11]:5701 - 0694c97c-df3d-4929-a629-aa06fd833888
}

12:51,879 INFO hazelcast.core.LifecycleService hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] HazelcastClient 3.11 (20181023 - 1500bbb) is CLIENT_CONNECTED
12:51,879 INFO internal.diagnostics.Diagnostics hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Diagnostics disabled. To enable add -Dhazelcast.diagnostics.enabled=true to the JVM arguments.
12:52,000 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.8-SNAPSHOT] [3.11] Authenticated with server [10.0.6.11]:5701, server version:3.11 Local address: /10.0.4.6:35002


/----------+--------------\
|       Top 5 Volumes     |
| Ticker   | Value        |
|----------+--------------|
| QCRH     | 32166850     |
| AMSF     | 32149667     |
| GOGL     | 31863102     |
| FNTEU    | 31834074     |
| VRSN     | 31832643     |
\---------+---------------/

```


