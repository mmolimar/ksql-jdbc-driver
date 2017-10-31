# KSQL JDBC Driver [![Build Status](https://travis-ci.org/mmolimar/ksql-jdbc-driver.svg?branch=master)](https://travis-ci.org/mmolimar/ksql-jdbc-driver)[![Coverage Status](https://coveralls.io/repos/github/mmolimar/ksql-jdbc-driver/badge.svg?branch=master)](https://coveralls.io/github/mmolimar/ksql-jdbc-driver?branch=master)

**ksql-jdbc-driver** is a Type 3 Java Database Connectivity (JDBC) driver that provides standard access to
Apache Kafka via JDBC API.

In the current version, the driver connects to the [KSQL engine](https://www.confluent.io/product/ksql/) to
perform queries to Kafka and then, the engine translates those requests to Kafka requests.

## Getting started

### Building from source ###

First of all, the KSQL lib has to be installed into your local repo (till now, there isn't a release available).

So, cloning the KSQL repo:

``git clone https://github.com/confluentinc/ksql.git && cd ksql && git checkout 0.1.x``

and installing it:

``mvn clean install -Dmaven.skip.test=true``

Once you did that, just have to clone the ksql-jdbc-driver repo and package it:
 
``git clone https://github.com/mmolimar/ksql-jdbc-driver.git && cd ksql-jdbc-driver``

``sbt clean package``

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

The URL has the form ``jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]``

where:

* **\<ksql-engine>**: represents the KSQL engine host.
* **\<port>**: is the KSQL engine port.
* **\<propertyN>**: are the custom client properties (optionals). Fow now, there is only one property and it's to
set if the KSQL connection is secured or not. The property name is ``secured`` and its value is a boolean
(``true``|``false``). Its default value is ``false``.

## TODO's

- [ ] Standalone mode: connecting directly to Kafka brokers.
- [ ] Make the driver more compliant with the JDBC spec.
- [ ] Enable a timeout when waiting for messages in a query.

## Contribute

- Source Code: https://github.com/mmolimar/ksql-jdbc-driver
- Issue Tracker: https://github.com/mmolimar/ksql-jdbc-driver/issues

## License

Released under the Apache License, version 2.0.