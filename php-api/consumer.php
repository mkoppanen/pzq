<?php

include 'PZQClient.php';

$c = new PZQConsumer ("tcp://127.0.0.1:11132");

for ($i = 0; $i < 10000; $i++)
{
    $message = $c->consume ();
    echo "Consuming " . ($i + 1) . PHP_EOL;
    $c->ack ($message);
}
