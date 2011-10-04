<?php

include 'PZQClient.php';

$p = new PZQProducer ("tcp://127.0.0.1:11131");

for ($i = 0; $i < 10000000; $i++)
{
    $message = new PZQMessage ();
    $message->set_id ("id-{$i}");
    $message->set_message (array ("hello there", "second"));
    echo "Produced id-{$i}" . PHP_EOL;

    $p->produce ($message, 10000);
}

