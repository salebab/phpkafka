<?php

if(!function_exists("kafka_produce")) {
    function kafka_produce($host, $topic, $message) {
        exit("kafka module not exists.");
    }
}
$start = microtime(1);
$messages = 1000;

for($i = 0; $i<$messages; $i++) {
    $result = kafka_produce("localhost:9092", "test00", md5(microtime(). rand()));
    //echo $result;

}

$time = round(microtime(1)-$start, 4);
echo "Produced $i in $time ms". PHP_EOL;