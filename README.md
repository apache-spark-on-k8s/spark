# Apache Spark On Kubernetes

This repository, located at https://github.com/apache-spark-on-k8s/spark, contains a fork of Apache Spark that enables running Spark jobs natively on a Kubernetes cluster.

## What is this?

This is a collaboratively maintained project working on [SPARK-18278](https://issues.apache.org/jira/browse/SPARK-18278). The goal is to bring native support for Spark to use Kubernetes as a cluster manager, in a fully supported way on par with the Spark Standalone, Mesos, and Apache YARN cluster managers.

## Getting Started

- [Usage guide](https://apache-spark-on-k8s.github.io/userdocs/) shows how to run the code
- [Development docs](resource-managers/kubernetes/README.md) shows how to get set up for development
- Code is primarily located in the [resource-managers/kubernetes](resource-managers/kubernetes) folder

## Why does this fork exist?

Adding native integration for a new cluster manager is a large undertaking.  If poorly executed, it could introduce bugs into Spark when run on other cluster managers, cause release blockers slowing down the overall Spark project, or require hotfixes which divert attention away from development towards managing additional releases.  Any work this deep inside Spark needs to be done carefully to minimize the risk of those negative externalities.

At the same time, an increasing number of people from various companies and organizations desire to work together to natively run Spark on Kubernetes.  The group needs a code repository, communication forum, issue tracking, and continuous integration, all in order to work together effectively on an open source product.

We've been asked by an Apache Spark Committer to work outside of the Apache infrastructure for a short period of time to allow this feature to be hardened and improved without creating risk for Apache Spark.  The aim is to rapidly bring it to the point where it can be brought into the mainline Apache Spark repository for continued development within the Apache umbrella.  If all goes well, this should be a short-lived fork rather than a long-lived one.

## Who are we?

This is a collaborative effort by several folks from different companies who are interested in seeing this feature be successful.  Companies active in this project include (alphabetically):

- Google
- Haiwen
- Hyperpilot
- Intel
- Palantir
- Pepperdata
- Red Hat