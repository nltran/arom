Welcome to AROM!
====

# Introduction

AROM is a distributed parallel processing framework based on the DFG model for the execution and the definition of the jobs. It is built around the actor model and uses AKKA 1.0 at the basis of its stack and is implemented in Scala.

# Getting Started

* Please check out our [wiki](https://github.com/nltran/arom/wiki), it is intended as our single point of information for the moment
* Widely annotated examples are available in the examples package

# Compilation Instructions
The current code on the repository is fully fonctional and comprises the master and the slave runtimes. Once 
repository is cloned all is needed to run AROM is a Java virtual machine.

To compile on UNIX systems, simply run

$ ./sbt update
$ ./sbt compile

# Configuration

The folowing configuration file must be adjusted:
 * aromslave/conf/akka.conf 
   (contains all AROM settings along those for the Akka library)

# Starting the slave runtime

The slave runtime can be launched with the command:

$ ./sbt 'project naiadslave' run

At the moment, the slave runtime needs to be manually launched on each of the slave nodes in the cluster.

The master runtime is automatically launched once a job is defined and started (please see wiki for examples and documentation)

# Contibutors
Tran Nam-Luc (Euranova)

Arthur Lesuisse (ULB - initial commit)