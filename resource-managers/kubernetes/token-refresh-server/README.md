---
layout: global
title: Hadoop Token Refresh Server on Kubernetes
---

Spark on Kubernetes may use Kerberized Hadoop data sources such as secure HDFS or Kafka. If the job
runs for days or weeks, someone should extend the lifetime of Hadoop delegation tokens, which
expire every 24 hours. The Hadoop Token Refresh Server is a Kubernetes microservice that renews
token lifetime and puts the replacement tokens in place.

# Building the Refresh Server

To build the refresh server jar, simply run Maven. For example:

    mvn clean package

The target directory will have a tarball that includes the project jar file as well as
3rd party dependency jars. The tarball name would end with `-assembly.tar.gz`. For example:

    target/token-refresh-server-kubernetes_2.11-2.2.0-k8s-0.3.0-SNAPSHOT-assembly.tar.gz

# Running the Refresh Server

To run the server, follow the steps below.

1. Build and push the docker image:

    docker build -t hadoop-token-refresh-server:latest  \
         -f src/main/docker/Dockerfile .
    docker tag hadoop-token-refresh-server:latest <YOUR-REPO>:<YOUR-TAG>
    docker push <YOUR-REPO>:<YOUR-TAG>

2. Edit the main application config file, src/main/conf/application.conf
   and create a `configmap`:

    kubectl create configmap hadoop-token-refresh-server-application-conf  \
        --from-file=src/main/conf/application.conf

3. Create another k8s `configmap` containing Hadoop config files. This should enable Kerberos and secure Hadoop.
   It should also include the Hadoop servers that would issue delegation tokens such as the HDFS namenode
   address:

    kubectl create configmap hadoop-token-refresh-server-hadoop-config  \
        --from-file=/usr/local/hadoop/conf/core-site.xml

4. Create yet another k8s `configmap` containing Kerberos config files. This should include
   the kerberos server address and the correct realm name for Kerberos principals:

    kubectl create configmap hadoop-token-refresh-server-kerberos-config  \
        --from-file=/etc/krb5.conf

5. Create a k8s `secret` containing the Kerberos keytab file. The keytab file should include
   the password for the system user Kerberos principal that the refresh server is using to
   extend Hadoop delegation tokens. See
   hadoop-token-refresh-server.kerberosPrincipal in the application.conf.

    kubectl create secret generic hadoop-token-refresh-server-kerberos-keytab  \
        --from-file /mnt/secrets/krb5.keytab

6. Optionally, create a k8s `service account` and `clusterrolebinding` that
   the service pod will use. The service account should have `edit` capability for
   job `secret`s that contains the Hadoop delegation tokens.

7. Finally, edit the config file for k8s `deployment` and launch the service pod
   using the deployment. The config file should include the right docker image tag
   and the correct k8s `service account` name.

    kubectl create -f src/main/conf/kubernetes-hadoop-token-refresh-server.yaml
