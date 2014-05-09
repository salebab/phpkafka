#ifdef ZTS
#include "TSRM.h"
#endif

#ifndef PHP_KAFKA_H
#define	PHP_KAFKA_H
#define PHP_KAFKA_VERSION "0.1"

extern zend_module_entry kafka_module_entry;

typedef struct {
  int length;
  int request_id;
  int response_to;
  int op;
} kafka_msg_header;

#define CREATE_MSG_HEADER(rid, rto, opcode) \
  header.length = 0; \
  header.request_id = rid; \
  header.response_to = rto; \
  header.op = opcode;

PHP_MSHUTDOWN_FUNCTION(kafka);
PHP_MINIT_FUNCTION(kafka);
PHP_RINIT_FUNCTION(kafka);
PHP_RSHUTDOWN_FUNCTION(kafka);

static PHP_METHOD(Kafka, __construct);
static PHP_METHOD(Kafka, produce);
static PHP_METHOD(Kafka, consume);
PHPAPI void kafka_connect(char *brokers);
#endif	/* PHP_KAFKA_H */