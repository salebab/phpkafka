#ifdef ZTS
#include "TSRM.h"
#endif

#ifndef PHP_KAFKA_H
#define	PHP_KAFKA_H
#define PHP_KAFKA_VERSION "0.1"

extern zend_module_entry kafka_module_entry;
PHP_MSHUTDOWN_FUNCTION(kafka);
PHP_MINIT_FUNCTION(kafka);
PHP_RINIT_FUNCTION(kafka);
PHP_RSHUTDOWN_FUNCTION(kafka);
//PHP_FUNCTION(kafka_setup);
//PHP_FUNCTION(kafka_produce);
//PHP_RSHUTDOWN_FUNCTION();
#endif	/* PHP_KAFKA_H */

