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
#include <inttypes.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <time.h>
#include "kafka.h"
#include "librdkafka/rdkafka.h"
typedef struct rd_kafka_topic_conf_s {
    int     required_acks;
        int     enforce_isr_cnt;
    int32_t request_timeout_ms;
    int     message_timeout_ms;

    int32_t (*partitioner) (const rd_kafka_topic_t *rkt,
                const void *keydata, size_t keylen,
                int32_t partition_cnt,
                void *rkt_opaque,
                void *msg_opaque);

        int     produce_offset_report;

        char   *group_id_str;
        void *group_id;    /* Consumer group id in protocol format */

    int     auto_commit;
    int     auto_commit_interval_ms;
    int     auto_offset_reset;
    char   *offset_store_path;
    int     offset_store_sync_interval_ms;
        enum {
                RD_KAFKA_OFFSET_METHOD_FILE,
                RD_KAFKA_OFFSET_METHOD_BROKER,
        } offset_store_method;

    /* Application provided opaque pointer (this is rkt_opaque) */
    void   *opaque;
} rd_kafka_topic_conf_t;
struct rd_kafka_topic_s {
    struct {
        struct rd_kafka_topic_s *tqe_next;
        struct rd_kafka_topic_s **tqe_prev;
    };

    int                rkt_refcnt;

    pthread_rwlock_t   rkt_lock;
    void *rkt_topic;
    struct rd_kafka_toppar_s  *rkt_ua;  /* unassigned partition */
    struct rd_kafka_toppar_s **rkt_p;
    int32_t            rkt_partition_cnt;
    struct {
        void * tqh_first;
        void * tqh_last;
    } rkt_desp;
    uint64_t             rkt_ts_metadata; /* Timestamp of last metadata
                         * update for this topic. */

    enum {
        RD_KAFKA_TOPIC_S_UNKNOWN,   /* No cluster information yet */
        RD_KAFKA_TOPIC_S_EXISTS,    /* Topic exists in cluster */
        RD_KAFKA_TOPIC_S_NOTEXISTS, /* Topic is not known in cluster */
    } rkt_state;

        int                rkt_flags;
#define RD_KAFKA_TOPIC_F_LEADER_QUERY  0x1 /* There is an outstanding
                                            * leader query for this topic */
    rd_kafka_t* rkt_rk;

    rd_kafka_topic_conf_t rkt_conf;
};
static int run = 1;
static rd_kafka_t *rk;
static rd_kafka_type_t rk_type;
static int exit_eof = 1; //Exit consumer when last message
char *brokers = "localhost:9092";
int64_t start_offset = 0;
int partition = RD_KAFKA_PARTITION_UA;

void kafka_connect(char *brokers)
{
    kafka_setup(brokers);
}

//return 1 if rd is not NULL
int kafka_is_connected( void )
{
    if (rk == NULL)
        return 0;
    return 1;
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
        //this wait is blocking PHP
        //not calling it will yield segfault, though
        rd_kafka_wait_destroyed(5);
        rk = NULL;
    }
}

static void kafka_init( rd_kafka_type_t type )
{
    if (rk_type != type) {
        kafka_destroy();
    }
    if (rk == NULL)
    {
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        if (!(rk = rd_kafka_new(type, conf, errstr, sizeof(errstr)))) {
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
}

void kafka_produce(char* topic, char* msg, int msg_len)
{

    signal(SIGINT, kafka_stop);
    signal(SIGPIPE, kafka_stop);

    rd_kafka_topic_t *rkt;
    int partition = RD_KAFKA_PARTITION_UA;

    rd_kafka_topic_conf_t *topic_conf;

    kafka_init(RD_KAFKA_PRODUCER);

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
      return NULL;
    }

    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - %% Consume error for topic \"%s\" [%"PRId32"] "
           "offset %"PRId64": %s\n",
           rd_kafka_topic_name(rkmessage->rkt),
           rkmessage->partition,
           rkmessage->offset,
           rd_kafka_message_errstr(rkmessage));
    return NULL;
  }

  //php_printf("%.*s\n", (int)rkmessage->len, (char *)rkmessage->payload);
  return rkmessage;
}

//get the available partitions for a given topic
void kafka_get_partitions(zval *return_value, char *topic)
{
    rd_kafka_topic_t *rkt;
    rd_kafka_topic_conf_t *conf;
    int i;//C89 compliant
    //connect if required
    //preserve current type
    kafka_init(rk_type);
    /* Topic configuration */
    conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, conf);

    for (i=0;i < (rkt)->rkt_partition_cnt;++i) {
        add_next_index_long(return_value, i);
    }
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

    kafka_init(RD_KAFKA_CONSUMER);

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
          continue;//so continue, or we'll get a segfault
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
      if (rkmessage_return != NULL) {
          if ((int) rkmessage_return->len > 0) {
              //ensure there is a payload
              char payload[(int) rkmessage_return->len];
              sprintf(payload, "%.*s", (int) rkmessage_return->len, (char *) rkmessage_return->payload);
              add_index_string(return_value, (int) rkmessage_return->offset, payload, 1);
          } else {
              //add empty value
              char payload[1] = "";//empty string
              add_index_string(return_value, (int) rkmessage_return->offset, payload, 1);
          }
      }
      /* Return message to rdkafka */
      rd_kafka_message_destroy(rkmessage);
    }

    /* Stop consuming */
    rd_kafka_consume_stop(rkt, partition);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);
}
