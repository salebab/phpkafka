#ifndef PHP_KAFKA_LIBRARY_H
#define	PHP_KAFKA_LIBRARY_H
void kafka_setup(char *brokers);
void kafka_produce(char* topic, char* msg, int msg_len);
void kafka_destroy();
#endif	/* PHP_KAFKA_LIBRARY_H */
