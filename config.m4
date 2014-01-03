dnl config.m4 for extension kafka

PHP_ARG_ENABLE(kafka, Whether to enable the "kafka" extension,
  [  --enable-kafka      Enable "kafka" extension support])

if test $PHP_KAFKA != "no"; then

  librdkafka_sources=" \
    librdkafka/rd.c \
    librdkafka/rdaddr.c \
    librdkafka/rdcrc32.c \
    librdkafka/rdgz.c \
    librdkafka/rdkafka.c \
    librdkafka/rdkafka_broker.c \
    librdkafka/rdkafka_defaultconf.c \
    librdkafka/rdkafka_msg.c \
    librdkafka/rdkafka_topic.c \
    librdkafka/rdlog.c \
    librdkafka/rdqueue.c \
    librdkafka/rdrand.c \
    librdkafka/rdthread.c \
    librdkafka/snappy.c"

  PHP_NEW_EXTENSION(kafka, kafka.c $librdkafka_sources, $ext_shared,,-I@ext_srcdir@/librdkafka)
  PHP_ADD_BUILD_DIR($ext_builddir/librdkafka)
  PHP_ADD_MAKEFILE_FRAGMENT
fi