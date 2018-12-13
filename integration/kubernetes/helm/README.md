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
$ helm install --name first-release --set managementcenter.licenseKey=<YOUR_LICENSE_KEY> hazelcast/hazelcast-jet
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
16:37,238 INFO impl.config.XmlJetConfigLocator Loading configuration /config/hazelcast-jet/hazelcast-client.xml from property hazelcast.client.config
16:37,239 INFO impl.config.XmlJetConfigLocator Using configuration file at /config/hazelcast-jet/hazelcast-client.xml
16:37,678 INFO hazelcast.core.LifecycleService hz.client_0 [dev] [0.7] [3.10.3] HazelcastClient 3.10.3 (20180718 - fec4eef) is STARTING
16:38,130 INFO discovery.integration.DiscoveryService hz.client_0 [dev] [0.7] [3.10.3] Kubernetes Discovery properties: { service-dns: null, service-dns-timeout: 5, service-name: first-release-hazelcast-jet, service-port: 0, service-label: null, service-label-value: true, namespace: default, resolve-not-ready-addresses: false, kubernetes-master: https://kubernetes.default.svc}
16:38,132 INFO discovery.integration.DiscoveryService hz.client_0 [dev] [0.7] [3.10.3] Kubernetes Discovery: Bearer Token { eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImRlZmF1bHQtdG9rZW4tNzZ0OW0iLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiZGVmYXVsdCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6ImI3Nzc3NDdkLWUzZmYtMTFlOC05MjljLTQyMDEwYTgwMDBmZSIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpkZWZhdWx0OmRlZmF1bHQifQ.TWo_UjuTzBjnldWT5Axo_vLkZh37OkXiDEmwJENchVOJAmIM-7KRuDAURwZEtxTGVmN-AhaSouoa-iqSqSEJDQYGjBmm0CbpVjvg-Hv2ubhCR9KxrIlDJRzbziiIQ1_N9UjN01LOrTITSJBHgk5uRxCxqLqw_0oMbvj69Ph8c15a2MtwgwVvzfAqK0Y0UkZ9mZ7jKNm2EutxwUMH9AwbbS5Qyi0VedvUYjnUhVUnJRru2CtXIWuPLE3_yFlp7piUNfBp2AOFtrwKsMXK8DsW5LArB0Waavlo51voDqjwTlO5c4PB0GyExzARxHJinm0bp2MVZxwCXZ_uWQrCpTg_nQ }
16:38,134 INFO discovery.integration.DiscoveryService hz.client_0 [dev] [0.7] [3.10.3] Kubernetes Discovery activated resolver: ServiceEndpointResolver
16:38,168 INFO client.spi.ClientInvocationService hz.client_0 [dev] [0.7] [3.10.3] Running with 2 response threads
16:38,262 INFO hazelcast.core.LifecycleService hz.client_0 [dev] [0.7] [3.10.3] HazelcastClient 3.10.3 (20180718 - fec4eef) is STARTED
16:39,016 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.7] [3.10.3] Trying to connect to [10.0.4.7]:5701 as owner member
16:39,075 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.7] [3.10.3] Setting ClientConnection{alive=true, connectionId=1, channel=NioChannel{/10.0.4.8:35671->/10.0.4.7:5701}, remoteEndpoint=[10.0.4.7]:5701, lastReadTime=2018-12-04 07:16:39.068, lastWriteTime=2018-12-04 07:16:39.064, closedTime=never, lastHeartbeatRequested=never, lastHeartbeatReceived=never, connected server version=3.10.5} as owner with principal ClientPrincipal{uuid='5b07f254-8d0d-44fa-8144-9aef9435cc67', ownerUuid='546c3065-c176-4e39-8687-09e76254bcd5'}
16:39,076 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.7] [3.10.3] Authenticated with server [10.0.4.7]:5701, server version:3.10.5 Local address: /10.0.4.8:35671
16:39,115 INFO spi.impl.ClientMembershipListener hz.client_0 [dev] [0.7] [3.10.3] 

Members [2] {
	Member [10.0.4.7]:5701 - 546c3065-c176-4e39-8687-09e76254bcd5
	Member [10.0.6.13]:5701 - 8558fedb-8737-45d5-bb9b-bbe1cb6e1e05
}

16:39,116 INFO hazelcast.core.LifecycleService hz.client_0 [dev] [0.7] [3.10.3] HazelcastClient 3.10.3 (20180718 - fec4eef) is CLIENT_CONNECTED
16:39,117 INFO internal.diagnostics.Diagnostics hz.client_0 [dev] [0.7] [3.10.3] Diagnostics disabled. To enable add -Dhazelcast.diagnostics.enabled=true to the JVM arguments.
16:39,253 INFO client.connection.ClientConnectionManager hz.client_0 [dev] [0.7] [3.10.3] Authenticated with server [10.0.6.13]:5701, server version:3.10.5 Local address: /10.0.4.8:44895

/----------+--------------\
|       Top 5 Volumes     |
| Ticker   | Value        |
|----------+--------------|
| PEGI     | 4109641      |
| MRAM     | 4076840      |
| HZNP     | 4050375      |
| VRSN     | 4048422      |
| BFIN     | 4018259      |
\---------+---------------/

```


