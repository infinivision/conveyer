
# bootstrap a debug environment for conveyer on single CentOS 7 host

```
$ sudo mkdir -p /opt/config /opt/log /opt/data/nsq
$ sudo chown -R 1000:1000 /opt/config /opt/log /opt/data/nsq
$ cp config/* /opt/config
$ sudo cp docker-compose.yml /opt
$ cd /opt
$ sudo docker-compose up -d
```

Goto nsqadmin GUI http://127.0.0.1:4171/lookup, create the "visits" topic.

# Reference
 - https://hub.docker.com/r/yandex/clickhouse-server/
