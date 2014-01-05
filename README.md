phpkafka
========

PHP extension for Apache Kafka is based on [librdkafka](https://github.com/edenhill/librdkafka/).

Installing
```bash
phpize
./configure --enable-kafka
make
sudo make install

```

Running:
```php
<?php
// Produce a message
kafka_produce("localhost:9092", "topic_name", "message content");

```
