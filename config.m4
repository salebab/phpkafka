dnl config.m4 for extension kafka

PHP_ARG_ENABLE(kafka, whether to enable the Kafka extension,
  [  --enable-kafka              Enable the Kafka extension])

if test $PHP_KAFKA != "no"; then
  LDFLAGS="$LDFLAGS -lrdkafka"
  PHP_ADD_LIBRARY("librdkafka")
  PHP_NEW_EXTENSION(kafka, php_kafka.c kafka.c library.c, $ext_shared)
fi