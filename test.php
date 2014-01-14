<?php

for($i = 0; $i<2; $i++) {

    $kafka = new \Kafka("54.235.79.229:19092");
    $kafka->produce("test123", $i);

}