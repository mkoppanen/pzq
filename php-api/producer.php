<?php

include 'PZQClient.php';

$p = new PZQProducer ("tcp://127.0.0.1:11131");

for ($i = 0; $i < 10000; $i++)
{
    $message = new PZQMessage ();
    $message->setId ($i);
    $message->setMessage (array ("hello there", "second"));

    $p->produce ($message);
}

