**Dremio Monitoring**

The objective of this example monitoring solution is to ensure overall Dremio
cluster health and performance. This example does not focus on query performance
optimization. The following diagram depicts a high level view of a Dremio
Cluster with dependencies and a monitoring solution deployed in Yarn. The
implementation steps and documentation are presented later in the document.

![](monitoring.png)

It’s important to approach the monitoring solution with a holistic approach and
monitor Dremio metrics, as well as metrics produced by related infrastructure,
such as nodes, data sources, load balancer, NFS, Zookeeper, etc.

Dremio provides various metrics suitable for monitoring via JMX interface,
Dremio API, and SQL. Metrics include coordinator-level metrics, executor-level
metrics, cluster-level metrics.
