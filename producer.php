<?php

dl ('zmq.so');

$z = new zmqcontext(); 
$s = new zmqsocket($z, zmq::SOCKET_REQ); 
$s->connect("tcp://127.0.0.1:11131");

sleep (1);

while (true)
{
	$s->sendMulti (array ("msg", "aa", "bb", "cc"));
    $s->recvmulti ();
}	
