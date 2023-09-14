# fluent-bit-clickhouse

Fluent-Bit go clickhouse output plugin

For example:

``` sh
docker run --rm -it \
    -v /var/log:/var/log \
    -v /data:/data \
    -e HOST_IP=10.206.65.167 \
    -e CLICKHOUSE_LOG_LEVEL=trace \
    -e CLICKHOUSE_HOSTS=<host1:port1,host2:port2,host3:port3> \
    -e CLICKHOUSE_DATABASE=<database> \
    -e CLICKHOUSE_USERNAME=<username> \
    -e CLICKHOUSE_PASSWORD=<password> \
    -e CLICKHOUSE_TABLE=<table> \
    -e CLICKHOUSE_COLUMNS=<column1,column2,column3...> \
    registry.cn-beijing.aliyuncs.com/llaoj/fluent-bit-clickhouse:v0.1.2 bash
```