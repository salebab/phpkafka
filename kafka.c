#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "rdkafka.h"  /* for Kafka driver */


ZEND_BEGIN_ARG_INFO(arginfo_kafka_produce, 0)
    ZEND_ARG_INFO(0, string)
ZEND_END_ARG_INFO();

/* Function names */
ZEND_FUNCTION(kafka_produce);

zend_function_entry kafka_functions[] =
{
    ZEND_FE(kafka_produce, arginfo_kafka_produce)
    {NULL,NULL,NULL} /* Marks the end of function entries */
};


zend_module_entry kafka_module_entry = {
    STANDARD_MODULE_HEADER,
    "kafka",
    kafka_functions, /* Function entries */
    NULL, /* Module init */
    NULL, /* Module shutdown */
    NULL, /* Request init */
    NULL, /* Request shutdown */
    NULL, /* Module information */
    "0.1", /* Replace with version number for your extension */
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_KAFKA
ZEND_GET_MODULE(kafka)
#endif

PHP_FUNCTION(kafka_produce)
{
    char* str;
    int str_len;
    
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &str, &str_len) == FAILURE) {
        return;
    }

    
    RETURN_STRING(str, 1);
}