
# Overview

Trino Query Log is a Trino plugin for logging query events into separate log file.

Its main purpose is to gather queries metadata and statistics as one event per line, so it can be easily collected by external software (e.g. Elastic FileBeat which will send data to Logstash/ElasticSearch/Kibana for storage/analysis).

## Requirements

- Jdk: 11
- Trino: 380

## Deploy

>  Using `trino-query-log-0.4-dist.tar.gz` as an example

### Download Binary File

https://github.com/archongum/trino-query-log/releases

### Unzip Plugin

```bash
tar xzvf trino-query-log-0.4-dist.tar.gz -C <TRINO_HOME>/plugin/ --strip-components=1 plugin/
````

### Prepare configuration file

Default configuration files:

```bash
tar xzvf trino-query-log-0.4-dist.tar.gz -C <TRINO_HOME>/etc/ --strip-components=1 etc/
````

Parameters Explain:

| Configuration                                            | Default                                        | Description                                                                                 | 
|----------------------------------------------------------|------------------------------------------------|---------------------------------------------------------------------------------------------|
| event-listener.name                                      | trino-query-log                                | String. Plugin Name, sample as plugin directory name                                        |
| trino.query.log.config.fileLocation                      | etc/event-listener-trino-query-log-logback.xml | String. Logback configuration xml                                                           |
| trino.query.log.log.splitCompletedEvent                  | true                                           | Boolean. See: [event-listener](https://trino.io/docs/current/develop/event-listener.html)   |
| trino.query.log.log.queryCreatedEvent                    | true                                           | Boolean. See: [event-listener](https://trino.io/docs/current/develop/event-listener.html)   |
| trino.query.log.log.queryCreatedEvent.queryTypePattern   | .*                                             | Regex. Only need these query types                                                          |
| trino.query.log.log.queryCreatedEvent.queryMaxLength     | -1                                             | Integer. Max string length for query and preparedQuery                                      |
| trino.query.log.log.queryCompletedEvent                  | true                                           | Boolean. See: [event-listener](https://trino.io/docs/current/develop/event-listener.html)   |
| trino.query.log.log.queryCompletedEvent.queryTypePattern | .*                                             | Regex. Only need these query types                                                          |
| trino.query.log.log.queryCompletedEvent.queryMaxLength   | -1                                             | Integer. Max string length for query and preparedQuery                                      |
| trino.query.log.log.queryCompletedEvent.catalogPattern   | .*                                             | Regex. Only need these catalogs                                                             |


## Build from Source

```bash
mvn clean package
```

Tarball `target/trino-query-log-<version>-dist.tar.gz`
