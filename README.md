# Cassandra output plugin for Embulk

Apache Cassandra output plugin for Embulk.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: yes
* **Cleanup supported**: no

## Caution
In current, version of netty components conflicts to one that is used by embulk-core.
This probrem is very severe.
I tested this plugin on embulk-0.9.7.
But future embulk version may break this plugin.

## Configuration

- **hosts**: list of seed hosts (list<string>, required)
- **port**: port number for cassandra cluster (integer, default: `9042`)
- **username**: cluster username (string, default: `null`)
- **password**: cluster password (string, default: `null`)
- **cluster_name**: cluster name (string, default: `null`)
- **keyspace**: target keyspace name (string, required)
- **table**: target table name (string, required)
- **if_not_exists**: Add "IF NOT EXISTS" to INSERT query (boolean, default: `false`)
- **ttl**: Add "TTL" to INSERT query (integer, default: `null`)
- **idempotent**: Treat INSERT query as idempotent (boolean, default: `false`)
- **connect_timeout**: Set connect timeout millisecond (integer, default: `5000`)
- **request_timeout**: Set each request timeout millisecond (integer, default: `12000`)

## Example

```yaml
out:
  type: cassandra
  hosts:
    - 127.0.0.1
  port: 9042
  keyspace: sample_keyspace
  table: sample_table
  idempotent: true
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
