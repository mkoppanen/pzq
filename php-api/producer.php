<?php

include 'PZQClient.php';

$p = new PZQProducer ("tcp://127.0.0.1:11131");

for ($i = 0; $i < 100000; $i++)
{
    $message = new PZQMessage ();
    $message->set_id ($i);
    $message->set_message (array ("first part", "order {$i}"));

    $p->produce ($message);
}

