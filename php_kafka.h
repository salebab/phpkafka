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
#ifndef PHP_KAFKA_H
#define	PHP_KAFKA_H 1

#define PHP_KAFKA_VERSION "0.1.0-dev"
#define PHP_KAFKA_EXTNAME "kafka"

extern zend_module_entry kafka_module_entry;

PHP_MSHUTDOWN_FUNCTION(kafka);
PHP_MINIT_FUNCTION(kafka);
PHP_RINIT_FUNCTION(kafka);
PHP_RSHUTDOWN_FUNCTION(kafka);

#ifdef ZTS
#include <TSRM.h>
#endif

/* Kafka class */
static PHP_METHOD(Kafka, __construct);
static PHP_METHOD(Kafka, __destruct);
static PHP_METHOD(Kafka, set_partition);
static PHP_METHOD(Kafka, setPartition);
static PHP_METHOD(Kafka, isConnected);
static PHP_METHOD(Kafka, disconnect);
static PHP_METHOD(Kafka, produce);
static PHP_METHOD(Kafka, consume);
PHPAPI void kafka_connect(char *brokers);

#endif
