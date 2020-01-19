# ksqlDB JDBC Driver [![Build Status](https://travis-ci.org/mmolimar/ksql-jdbc-driver.svg?branch=master)](https://travis-ci.org/mmolimar/ksql-jdbc-driver)[![Coverage Status](https://coveralls.io/repos/github/mmolimar/ksql-jdbc-driver/badge.svg?branch=master)](https://coveralls.io/github/mmolimar/ksql-jdbc-driver?branch=master)

**ksql-jdbc-driver** is a Type 3 Java Database Connectivity (JDBC) driver that provides standard access to
Apache Kafka via JDBC API.

In the current version, the driver connects to the [ksqlDB engine](https://ksqldb.io/) to
perform queries to Kafka and then, the engine translates those requests to Kafka requests.

## Getting started

### Building from source ###

Just clone the ``ksql-jdbc-driver`` repo and package it:

``git clone https://github.com/mmolimar/ksql-jdbc-driver.git && cd ksql-jdbc-driver``

``sbt clean package``

If you want to build a fat jar containing both classes and dependencies -for instance, to use it in a
JDBC client such as [SQuirrel SQL](http://squirrel-sql.sourceforge.net/) or whichever-, type the following:

``sbt clean assembly``

### Running tests ###

To run unit and integration tests, execute the following:

``sbt test it:test``

#### Coverage ###

To know the test coverage of the driver:

``sbt clean coverage test it:test coverageReport``

## Usage

As expected, the driver can be used as we are used to. So, in your application, register the driver (depending on
your JVM), for example:

* ``java.sql.DriverManager.registerDriver(new com.github.mmolimar.ksql.jdbc.KsqlDriver)``

or

* ``Class.forName("com.github.mmolimar.ksql.jdbc.KsqlDriver")``

### Connection URL

The URL has the form ``jdbc:ksql://[<username>:<password>@]<ksql-engine>:<port>[?<property1>=<value>&<property2>=<value>...]``

where:

* **\<username>:\<password>**: optional username and password to log into ksqlDB.
* **\<ksql-engine>**: represents the ksqlDB engine host.
* **\<port>**: ksqlDB engine port.
* **\<propertyN>**: are the custom client properties (optionals). Available properties:
  * ``secured``: sets if the ksqlDB connection is secured or not. It's a boolean (``true``|``false``) and its default
  value is ``false``.
  * ``properties``: enables to set in ksqlDB extra properties from the JDBC URL. It's a boolean (``true``|``false``)
  and its default value is ``false``.
  * ``timeout``: sets the max wait time between each message when receiving them. It's a long and its default
  value is ``0`` which means that is infinite.

## TODO's

- [ ] Standalone mode: connecting directly to Kafka brokers.
- [ ] Make the driver more compliant with the JDBC spec.

## Contribute

- Source Code: https://github.com/mmolimar/ksql-jdbc-driver
- Issue Tracker: https://github.com/mmolimar/ksql-jdbc-driver/issues

## License

Released under the Apache License, version 2.0.
