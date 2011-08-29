<?php

dl ('zmq.so');

$z = new zmqcontext(); 
$s = new zmqsocket($z, zmq::SOCKET_PULL);
$s->setsockopt (ZMQ::SOCKOPT_HWM, 1);
$s->connect("tcp://127.0.0.1:11133"); 

$ack = new zmqsocket($z, zmq::SOCKET_PUSH); 
$ack->connect("tcp://127.0.0.1:11132");

//for ($i = 0; $i < 5; $i++) 

$i = 0;
while (true)
{
    $response = $s->recvMulti();
    $ack->send ($response [0]);
    echo "Handled message $i" . PHP_EOL;
    $i++;
}

	


