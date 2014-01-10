<?php

if(!function_exists("kafka_produce")) {
    function kafka_produce($host, $topic, $message) {
        exit("kafka module not exists.");
    }
}
$start = microtime(1);
$messages = 10000;

for($i = 0; $i<$messages; $i++) {
    $result = kafka_produce("localhost:9092", "test00", md5(microtime(). rand()));
}
echo "Sleeping..". PHP_EOL;
sleep(1);
echo "End sleeping!\n";
$time = round(microtime(1)-$start, 4);
echo "Produced $i in $time ms". PHP_EOL;
//sleep(3);