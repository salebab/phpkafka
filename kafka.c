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
#include <php.h>
#include "kafka.h"
#include <php_kafka.h>

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <syslog.h>
#include <time.h>
#include "kafka.h"
#include "librdkafka/rdkafka.h"

static int run = 1;
static rd_kafka_t *rk;
static int exit_eof = 1; //Exit consumer when last message
char *brokers = "localhost:9092";
int64_t start_offset = 0;
int partition = RD_KAFKA_PARTITION_UA;

void kafka_connect(char *brokers)
{
    kafka_setup(brokers);
}

void kafka_set_partition(int partition_selected)
{
    partition = partition_selected;
}

void kafka_stop(int sig) {
    run = 0;
    fclose(stdin); /* abort fgets() */
    rd_kafka_destroy(rk);
    rk = NULL;
}

void kafka_err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - ERROR CALLBACK: %s: %s: %s\n",
            rd_kafka_name(rk), rd_kafka_err2str(err), reason);

    kafka_stop(err);
}

void kafka_msg_delivered (rd_kafka_t *rk,
                           void *payload, size_t len,
                           int error_code,
                           void *opaque, void *msg_opaque) {
    if (error_code) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - Message delivery failed: %s",
                rd_kafka_err2str(error_code));
    }
}

void kafka_setup(char* brokers_list)
{
    brokers = brokers_list;
}

void kafka_destroy()
{
    if(rk != NULL) {
        rd_kafka_destroy(rk);
        rd_kafka_wait_destroyed(1000);
        rk = NULL;
    }
}

void kafka_produce(char* topic, char* msg, int msg_len)
{

    signal(SIGINT, kafka_stop);
    signal(SIGPIPE, kafka_stop);

    rd_kafka_topic_t *rkt;
    int partition = RD_KAFKA_PARTITION_UA;

    rd_kafka_topic_conf_t *topic_conf;

    if(rk == NULL) {
        char errstr[512];
        rd_kafka_conf_t *conf;

        /* Kafka configuration */
        conf = rd_kafka_conf_new();

        if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "phpkafka - failed to create new producer: %s", errstr);
                exit(1);
        }

        /* Add brokers */
        if (rd_kafka_brokers_add(rk, brokers) == 0) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "php kafka - No valid brokers specified");
                exit(1);
        }

        /* Set up a message delivery report callback.
         * It will be called once for each message, either on successful
         * delivery to broker, or upon failure to deliver to broker. */
        rd_kafka_conf_set_dr_cb(conf, kafka_msg_delivered);
        rd_kafka_conf_set_error_cb(conf, kafka_err_cb);

        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - using: %s", brokers);
    }

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);

    if (rd_kafka_produce(rkt, partition,
                     RD_KAFKA_MSG_F_COPY,
                     /* Payload and length */
                     msg, msg_len,
                     /* Optional key and its length */
                     NULL, 0,
                     /* Message opaque, provided in
                      * delivery report callback as
                      * msg_opaque. */
                     NULL) == -1) {
      openlog("phpkafka", 0, LOG_USER);
      syslog(LOG_INFO, "phpkafka - %% Failed to produce to topic %s "
          "partition %i: %s",
          rd_kafka_topic_name(rkt), partition,
          rd_kafka_err2str(
            rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0);
    }

    /* Poll to handle delivery reports */
    rd_kafka_poll(rk, 0);

    /* Wait for messages to be delivered */
    while (run && rd_kafka_outq_len(rk) > 0)
      rd_kafka_poll(rk, 100);

    rd_kafka_topic_destroy(rkt);
}

static rd_kafka_message_t *msg_consume(rd_kafka_message_t *rkmessage,
       void *opaque) {
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      openlog("phpkafka", 0, LOG_USER);
      syslog(LOG_INFO,
        "phpkafka - %% Consumer reached end of %s [%"PRId32"] "
             "message queue at offset %"PRId64"\n",
             rd_kafka_topic_name(rkmessage->rkt),
             rkmessage->partition, rkmessage->offset);
      if (exit_eof)
        run = 0;
      return;
    }

    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - %% Consume error for topic \"%s\" [%"PRId32"] "
           "offset %"PRId64": %s\n",
           rd_kafka_topic_name(rkmessage->rkt),
           rkmessage->partition,
           rkmessage->offset,
           rd_kafka_message_errstr(rkmessage));
    return;
  }

  //php_printf("%.*s\n", (int)rkmessage->len, (char *)rkmessage->payload);
  return rkmessage;
}

void kafka_consume(zval* return_value, char* topic, char* offset, int item_count)
{

  int read_counter = 0;

  if (strlen(offset) != 0) {
    if (!strcmp(offset, "end"))
      start_offset = RD_KAFKA_OFFSET_END;
    else if (!strcmp(offset, "beginning"))
      start_offset = RD_KAFKA_OFFSET_BEGINNING;
    else if (!strcmp(offset, "stored"))
      start_offset = RD_KAFKA_OFFSET_STORED;
    else
      start_offset = strtoll(offset, NULL, 10);
  }

    rd_kafka_topic_t *rkt;

    char errstr[512];
    rd_kafka_conf_t *conf;

    /* Kafka configuration */
    conf = rd_kafka_conf_new();

    /* Create Kafka handle */
    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)))) {
                  openlog("phpkafka", 0, LOG_USER);
                  syslog(LOG_INFO, "phpkafka - failed to create new consumer: %s", errstr);
                  exit(1);
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(rk, brokers) == 0) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "php kafka - No valid brokers specified");
            exit(1);
    }

    rd_kafka_topic_conf_t *topic_conf;

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);

    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - start_offset: %"PRId64" and offset passed: %s", start_offset, offset);

    /* Start consuming */
    if (rd_kafka_consume_start(rkt, partition, start_offset) == -1) {
      openlog("phpkafka", 0, LOG_USER);
      syslog(LOG_INFO, "phpkafka - %% Failed to start consuming: %s",
        rd_kafka_err2str(rd_kafka_errno2err(errno)));
      exit(1);
    }

    if (item_count != 0) {
      read_counter = item_count;
    }

    while (run) {
      if (item_count != 0 && read_counter >= 0) {
        read_counter--;
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - read_counter: %d", read_counter);
        if (read_counter == -1) {
          run = 0;
          continue;
        }
      }

      rd_kafka_message_t *rkmessage;

      /* Consume single message.
       * See rdkafka_performance.c for high speed
       * consuming of messages. */
      rkmessage = rd_kafka_consume(rkt, partition, 1000);
      if (!rkmessage) /* timeout */
        continue;

      rd_kafka_message_t *rkmessage_return;
      rkmessage_return = msg_consume(rkmessage, NULL);
      char payload[(int)rkmessage_return->len];
      sprintf(payload, "%.*s", (int)rkmessage_return->len, (char *)rkmessage_return->payload);
      add_index_string(return_value, (int)rkmessage_return->offset, payload, 1);

      /* Return message to rdkafka */
      rd_kafka_message_destroy(rkmessage);
    }

    /* Stop consuming */
    rd_kafka_consume_stop(rkt, partition);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);
}
