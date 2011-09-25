<?php
/*
 *  Copyright 2011 Mikko Koppanen <mikko@kuut.io>
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.                 
 */

class PZQClientException extends Exception {}
 
class PZQMessage
{
    private $_id = null;
    
    private $_message = null;

    public function getId ()
    {
        return $this->_id;
    }
    
    public function setId ($id)
    {
        return $this->_id = $id;
    }
    
    public function getMessage ()
    {
        return $this->_message;
    }
    
    public function setMessage ($message)
    {
        $this->_message = $message;
    }
}

class PZQProducer 
{
    private $socket;
    
    private $poll;
    
    public function __construct ($dsn)
    {
        $this->socket = new ZMQSocket (new ZMQContext (), ZMQ::SOCKET_XREQ);
        $this->socket->connect ($dsn);
        
        $this->poll = new ZMQPoll ();
        $this->poll->add ($this->socket, ZMQ::POLL_IN);
    }
    
    public function produce (PZQMessage $message, $timeout = 5000)
    {
        $out = array ($message->getId (), "");
        $m = $message->getMessage ();
        
        if (is_array ($m))
            $out = array_merge ($out, $m);
        else
            array_push ($out, $m);
        
        $this->socket->sendMulti ($out);
        
        $r = $w = array ();
        $this->poll->poll ($r, $w, 5000);
        
        if (empty ($r))
            throw new PZQClientException ('ACK timeout');
        
        $response = $this->socket->recvMulti ();
        
        if ($response [1] != $message->getId ()) 
            throw new PZQClientException ('Got ACK for wrong message');

        if ($response [2] != 'OK')
            throw new PZQClientException (
                        'Remote peer failed to handle message'
                      );
                      
        return true;
    }
}

class PZQConsumer 
{
    private $socket;
    
    private $ack_socket;
    
    public function __construct ($dsn, $ack_dsn, $is_pub = false)
    {
        $ctx = new ZMQContext ();
        
        $this->socket = new ZMQSocket (
                            $ctx, 
                            ($is_pub ? ZMQ::SOCKET_SUB : ZMQ::SOCKET_PULL));

        if ($is_pub)
            $this->socket->setSockOpt (ZMQ::SOCKOPT_SUBSCRIBE, "");

        $this->socket->connect ($dsn);
        
        $this->ack_socket = new ZMQSocket ($ctx, ZMQ::SOCKET_PUSH);
        $this->ack_socket->connect ($ack_dsn);
    }
    
    public function consume ($block = true)
    {
        $parts = $this->socket->recvMulti (($block ? 0 : ZMQ::MODE_NOBLOCK));
        
        if ($parts === false)
            return false;

        $this->ack_socket->send ($parts [0]);

        $message = new PZQMessage ();
        $message->setId ($parts [0]);
        $message->setMessage (array_slice ($parts, 2));
        return $message;
    }
}

class PZQMonitor
{
    private $socket;

    public function __construct ($dsn)
    {
        $ctx = new ZMQContext ();
        
        $this->socket = new ZMQSocket ($ctx, ZMQ::SOCKET_REQ);
        $this->socket->connect ($dsn);
    }
    
    public function get_stats ()
    {
        $this->socket->send ("MONITOR");
        
        $message = $this->socket->recv ();
        $parts = explode ("\n", $message);
        $parts = array_filter ($parts);
        return $parts;
    }
}

