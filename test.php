<?php

if(!function_exists("kafka_produce")) {
    function kafka_produce($host, $topic, $message) {
        exit("kafka module not exists.");
    }
}
$start = microtime(1);
$messages = 2;

for($i = 0; $i<$messages; $i++) {
    $result = kafka_produce("54.197.226.27:19092", "test123", md5(microtime(). rand()));
}
$time = round(microtime(1)-$start, 4);
echo "Produced $i in $time ms". PHP_EOL;
//sleep(3);