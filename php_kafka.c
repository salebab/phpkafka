/**
 *  Copyright 2013-2014 Patrick Reilly.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <php.h>
#include <php_kafka.h>
#include "kafka.h"
#include "zend_exceptions.h"

/* decalre the class entry */
zend_class_entry *kafka_ce;

/* the method table */
/* each method can have its own parameters and visibility */
static zend_function_entry kafka_functions[] = {
    PHP_ME(Kafka, __construct, NULL, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, __destruct, NULL, ZEND_ACC_DTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, set_partition, NULL, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
    PHP_ME(Kafka, setPartition, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, disconnect, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, produce, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Kafka, consume, NULL, ZEND_ACC_PUBLIC)
    {NULL,NULL,NULL} /* Marks the end of function entries */
};

zend_module_entry kafka_module_entry = {
    STANDARD_MODULE_HEADER,
    "kafka",
    kafka_functions, /* Function entries */
    PHP_MINIT(kafka), /* Module init */
    PHP_MSHUTDOWN(kafka), /* Module shutdown */
    PHP_RINIT(kafka), /* Request init */
    PHP_RSHUTDOWN(kafka), /* Request shutdown */
    NULL, /* Module information */
    PHP_KAFKA_VERSION, /* Replace with version number for your extension */
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_KAFKA
ZEND_GET_MODULE(kafka)
#endif

#ifndef BASE_EXCEPTION
#if (PHP_MAJOR_VERSION < 5) || ( ( PHP_MAJOR_VERSION == 5 ) && (PHP_MINOR_VERSION < 2) )
#define BASE_EXCEPTION zend_exception_get_default()
#else
#define BASE_EXCEPTION zend_exception_get_default(TSRMLS_C)
#endif
#endif

PHP_MINIT_FUNCTION(kafka)
{
    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "Kafka", kafka_functions);
    kafka_ce = zend_register_internal_class(&ce TSRMLS_CC);
    zend_declare_property_null(kafka_ce, "partition", sizeof("partition") -1, ZEND_ACC_PRIVATE TSRMLS_CC);
    zend_declare_class_constant_stringl(
        kafka_ce, "OFFSET_BEGIN", sizeof("OFFSET_BEGIN") -1,
        "beginning", sizeof("beginning")-1
    );
    zend_declare_class_constant_stringl(
        kafka_ce, "OFFSET_END", sizeof("OFFSET_END")-1,
        "end", sizeof("end")
    );
    return SUCCESS;
}
PHP_RSHUTDOWN_FUNCTION(kafka) { return SUCCESS; }
PHP_RINIT_FUNCTION(kafka) { return SUCCESS; }
PHP_MSHUTDOWN_FUNCTION(kafka) {
    kafka_destroy();
    return SUCCESS;
}

PHP_METHOD(Kafka, __construct)
{
    char *brokers = "localhost:9092";
    int brokers_len;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|s",
            &brokers, &brokers_len) == FAILURE) {
        return;
    }

    kafka_connect(brokers);
}

PHP_METHOD(Kafka, __destruct)
{
    if (kafka_is_connected()) {
        kafka_destroy();
    }
}

PHP_METHOD(Kafka, set_partition)
{
    zval *partition;

    if (
            zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z", &partition) == FAILURE
        ||
            Z_TYPE_P(partition) != IS_LONG
    ) {
        zend_throw_exception(BASE_EXCEPTION, "Partition is expected to be an int", 0 TSRMLS_CC);
        return;
    }
    kafka_set_partition(Z_LVAL_P(partition));
    //update partition property, so we can check to see if it's set when consuming
    zend_update_property(kafka_ce, getThis(), "partition", sizeof("partition") -1, partition TSRMLS_CC);
}

//leave duplicate method for now, set_partition is deprecated
PHP_METHOD(Kafka, setPartition)
{
    zval *partition;

    if (
            zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|z", &partition) == FAILURE
        ||
            Z_TYPE_P(partition) != IS_LONG
    ) {
        zend_throw_exception(BASE_EXCEPTION, "Partition is expected to be an int", 0 TSRMLS_CC);
        return;
    }
    kafka_set_partition(Z_LVAL_P(partition));
    zend_update_property(kafka_ce, getThis(), "partition", sizeof("partition") -1, partition TSRMLS_CC);
}

PHP_METHOD(Kafka, disconnect)
{
    kafka_destroy();
    if (kafka_is_connected()) {
        RETURN_FALSE;
    }
    RETURN_TRUE;
}

PHP_METHOD(Kafka, produce)
{
    zval *object = getThis(),
        *partition;
    char *topic;
    char *msg;
    int topic_len;
    int msg_len;
    int prod_val;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss",
            &topic, &topic_len,
            &msg, &msg_len) == FAILURE) {
        return;
    }

    prod_val = kafka_produce(topic, msg, msg_len);
    if (prod_val == -1) {
        RETURN_FALSE;
    }
    RETURN_LONG(prod_val);
}

PHP_METHOD(Kafka, consume)
{
    zval *object = getThis(),
        *partition;
    char *topic;
    int topic_len;
    char *offset;
    int offset_len;
    long item_count = 0;

    partition = zend_read_property(kafka_ce, object, "partition", sizeof("partition") -1, 0 TSRMLS_CC);
    if (Z_TYPE_P(partition) == IS_NULL) {
        //TODO: throw exception, trigger error, fallback to default (0) partition...
        //for now, default to 0
        kafka_set_partition(0);
        ZVAL_LONG(partition, 0);
        //update property value ->
        zend_update_property(kafka_ce, object, "partition", sizeof("partition") -1, partition TSRMLS_CC);
    }
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|sl",
            &topic, &topic_len,
            &offset, &offset_len,
            &item_count) == FAILURE) {
        return;
    }

    array_init(return_value);
    kafka_consume(return_value, topic, offset, item_count);

    if(return_value == NULL) {
        RETURN_FALSE;
    }
}
