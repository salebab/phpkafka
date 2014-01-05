phpkafka
========

PHP extension for Apache Kafka is based on [librdkafka](https://github.com/edenhill/librdkafka/).

Installing
```bash
phpize
./configure --enable-kafka
make
sudo make install
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/conf.d/kafka.ini'
```

Running:
```php
<?php
// Produce a message
kafka_produce("localhost:9092", "topic_name", "message content");
```
