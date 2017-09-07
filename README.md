# Tensei-Agent

![TeamCity Build Status](https://teamcity.wegtam.com/TeamCity/app/rest/builds/buildType%3A%28id%3ATENSEI_AGENT_TESTS%29/statusIcon)
[![Travis CI Build Status](https://travis-ci.org/Tensei-Data/tensei-agent.svg?branch=master)](https://travis-ci.org/Tensei-Data/tensei-agent)
[![codecov](https://codecov.io/gh/Tensei-Data/tensei-agent/branch/master/graph/badge.svg)](https://codecov.io/gh/Tensei-Data/tensei-agent)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/a66bcb5d504f467da5f929612d775b2c)](https://www.codacy.com/app/jan0sch/tensei-agent)

An agent is the workhorse of the Tensei (転成) system. It uses an actor 
system to do the actual work of reading, parsing, transforming and 
writing the data.

It communicates with the tensei-server which is responsible for starting 
and stopping and agent.

## Resources

The main website for Tensei-Data is located at: https://www.wegtam.com/products/tensei-data

### Mailing lists

[![Google-Group tensei-data](https://img.shields.io/badge/group-tensei--data-brightgreen.svg)](https://groups.google.com/forum/#!forum/tensei-data)
[![Google-Group tensei-data-dev](https://img.shields.io/badge/group-tensei--data--dev-orange.svg)](https://groups.google.com/forum/#!forum/tensei-data-dev)

## System architecture and provisioning

The Tensei-Data system is build upon three components:

1. Tensei-Server
2. Tensei-Frontend
3. At least one Tensei-Agent

To be able to run Tensei-Data you have to start at least one of each components.

For development purposes it is feasible to simply start each one from the sbt prompt via the `run` task.

### Provisioning / Deployment

To be able to provision the system components a packaging configuration for the [sbt native packager](https://github.com/sbt/sbt-native-packager) plugin is included. The recommended way is to create debian packages via the `debian:packageBin` sbt task. Resulting debian packages can be installed on a debian or ubuntu system. Before the package is build the test suite will be executed.

    % sbt clean debian:packageBin

We recommend to use the `gdebi` tool on ubuntu because it will automatically fetch required dependencies.

The packages include system startup scripts that will launch them upon system boot.

## Testing

There are tests (`sbt test`) and integration tests (`sbt it:test`) that can
be executed by the appropriate sbt tasks. The integration tests are tagged
according to their requirements. Please see `com.wegtam.scalatest.tags`
package.

To run only tests with a specific test use the `-n` flag:

```
> it:testOnly -- -n com.wegtam.scalatest.tags.DbTestH2
```

To run all tests **except** the ones with specific tags use the `-l` flag:

```
> it:testOnly -- -l com.wegtam.scalatest.tags.DbTestFirebird
```

## Problematic dependencies

### Hyperic Sigar

We use a JAR file that is provided by our own repository. To be able to use
the hyperic sigar extension properly the system property `-Djava.library.path`
must be set to the folder were the system library of sigar is installed.
This can simply be done via the activator:

    % activator -Djava.library.path=/usr/local/share/java/classes run

The command above should work on a FreeBSD system that has the sigar port 
installed.

## Profiling

To profile the agent several JVM options should be included. These can
be specified directly on the command line if `activator` is used:

    % activator -J-XX:+PrintGCApplicationStoppedTime run

### Benchmarks

The sub module `benchmarks` includes benchmarks that can be started 
using the activator/sbt console.

#### JMH Benchmarks

To compile and run the JMH benchmarks just issue the following commands:

    > benchmarks/jmh:clean
    > benchmarks/jmh:compile
    > benchmarks/jmh:run -i 10 -wi 4 -f3 -t1

#### Memory benchmarks

To compile and run the Jamm memory benchmarks issue the following commands:

    > benchmarks/clean
    > benchmarks/compile
    > benchmarks/run

Choose a benchmark to run and remember that you might need to increase
the memory settings for the JVM (see `javaOptions in run` in `build.sbt`).

### jHiccup

The best way to use [jHiccup](https://github.com/giltene/jHiccup) is
probably to use the `javaagent` parameter:

    % activator -J-javaagent:/path/to/jHiccup.jar run

The usage of jHiccup should always be combined with GC analysis.

    % activator -J-javaagent:/path/to/jHiccup.jar -J-XX:+PrintGCApplicationStoppedTime run

### Memory analysis

For memory (heap) analysis you should specify memory settings and instruct 
the jvm to write a heap dump if an out of memory error occurs.

    % activator -J-server -J-Xms1g -J-Xmx1g -J-XX:MaxMetaspaceSize=1g -J-XX:+HeapDumpOnOutOfMemoryError -J-XX:HeapDumpPath=/tmp/jvm-dumps run

Alternatively you can create heap dumps via the jmap utility.

    % jmap -dump:format=b,file=FILENAME.hprof PID

The resulting hprof files can be analysed using the [Eclipse Memory
Analyzer](https://eclipse.org/mat/). Beware that eclipse will need *huge*
ammounts of memory!

