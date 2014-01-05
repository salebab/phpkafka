dnl config.m4 for extension kafka

PHP_ARG_ENABLE(kafka, Whether to enable the "kafka" extension,
  [  --enable-kafka      Enable "kafka" extension support])

if test $PHP_KAFKA != "no"; then
  LDFLAGS="$LDFLAGS -lrdkafka"
  PHP_ADD_LIBRARY("librdkafka")
  PHP_NEW_EXTENSION(kafka, kafka.c, $ext_shared)
fi