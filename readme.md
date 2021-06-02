# IMMUTABLE BTREE

* History: must be implemented as a separated index with no history (to stop the recursion)
* <b>Abstract serialization (as a separated concept). It keeps the index implementation simpler. How cache has retrieved a block? Does not matter if did it by taking it from memory, from a service or from disk! The index works only with JVM objects.</b>

**HOW TO RUN (Cassandra Storage version)**

1. Tools needed:
- Docker
- JDK 1.8 or superior (ideally JDK 15)
- SBT build tool (1.4.5) (Scala)
- YugaByteDB (Cassandra like database)

2. Installing YugaByte (this takes some time! Be patient):

   $ docker pull yugabytedb/yugabyte
   $ docker-compose -f ./yugabytedb.yaml up -d

   After installation, open command line application and execute:
   $ docker exec -it yb-tserver-n1 /bin/bash
   $ cd bin
   $ ./cqlsh

   Copy the content of cassandra_keyspace.cql and paste in the command prompt!
   Hit enter.

   $ The database management web interface can be accessed at http://localhost:7000/

3.  To run the test: 

    $ sbt "testOnly services.scalable.index.MainSpec"

