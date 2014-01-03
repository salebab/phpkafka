PHP_ARG_WITH(kafka, for kafka support,
[  --with-kafka       Include kafka support. File is the optional path to kafka-config])

if test "$PHP_KAFKA" != "no"; then
  PHP_NEW_EXTENSION(kafka, kafka.c, $ext_shared)
fi