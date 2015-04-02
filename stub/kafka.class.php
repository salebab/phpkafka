<?php

final class Kafka
{
    const OFFSET_BEGIN = 'beginning';
    const OFFSET_END = 'end';

    /**
     * This property does not exist, connection status
     * Is obtained directly from C kafka client
     * @var bool
     */
    private $connected = false;

    /**
     * @var int
     */
    private $partition = 0;

    public function __construct($brokers = 'localhost:9092')
    {};

    /**
     * @param int $partition
     * @deprecated use setPartition instead
     */
    public function set_partition($partition)
    {
        $this->partition = $partition;
    }

    /**
     * @param int $partition
     */
    public function setPartition($partition)
    {
        $this->partition = $partition;
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return $this->connected;
    }

    /**
     * produce message on topic
     * @param string $topic
     * @param string $message
     * @return bool|int
     */ 
    public function produce($topic, $message)
    {
        $this->connected = true;
        //internal call, produce message on topic
        if ($this->partition < 0) {
            return false;//produce failed
        }
        return $this->partition;
    }

    /**
     * @param string $topic
     * @param string|int $offset
     * @param string|int $count
     * @return array
     */
    public function consume($topic, $offset = self::OFFSET_BEGIN, $count = self::OFFSET_END)
    {
        $this->connected = true;
        $return = [];
        if (!is_numeric($offset)) {
            //0 or last message (whatever its offset might be)
            $start = $offset == self::OFFSET_BEGIN ? 0 : 100;
        } else {
            $start = $offset;
        }
        if (!is_numeric($count)) {
            //depending on amount of messages in topic
            $count = 100;
        }
        return array_fill_keys(
            range($start, $start + $count),
            'the message at the offset $key'
        );
    }

    public function __destruct()
    {
        $this->connected = false;
    }
}
