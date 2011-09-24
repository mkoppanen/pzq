<?php

dl ('zmq.so');

$z = new zmqcontext(); 
$s = new zmqsocket($z, zmq::SOCKET_XREQ); 
$s->connect("tcp://127.0.0.1:11131");

$poll = new ZMQPoll();
$poll->add ($s, ZMQ::POLL_IN);

sleep (1);

for ($i = 0; $i < 10000000; $i++)
{
    $s->sendMulti (array ("msg", "aa", "bb", "cc"));
    
    $readable = $writable = array ();
    $poll->poll ($readable, $writable, 10000);
    
    if (count ($readable))
        var_dump ($s->recvmulti ());

    echo "Sent message " . ($i + 1) . PHP_EOL;
}	
