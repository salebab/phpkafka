<?php

for($i = 0; $i<2; $i++) {

    $kafka = new \Kafka("localhost:9092");
    $kafka->produce("test123", $i);

}