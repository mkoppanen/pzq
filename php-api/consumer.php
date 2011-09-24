<?php

include 'PZQClient.php';

$c = new PZQConsumer ("tcp://127.0.0.1:11133", "tcp://127.0.0.1:11132");

for ($i = 0; $i < 10000; $i++)
    $c->consume ();