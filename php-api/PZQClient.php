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
    
    private $_origin = null;
    
    private $_peer = null;
    
    private $_message = null;

    public function getId ()
    {
        return $this->_id;
    }
    
    public function setId ($id)
    {
        return $this->_id = $id;
    }
    
    public function getOrigin ()
    {
        return $this->_origin;
    }
    
    public function setOrigin ($origin)
    {
        return $this->_origin = $origin;
    }
    
    public function getPeer ()
    {
        return $this->_peer;
    }
    
    public function setPeer ($peer)
    {
        return $this->_peer = $peer;
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

    public function __construct ($dsn, $is_pub = false)
    {
        $ctx = new ZMQContext ();
        
        $this->socket = new ZMQSocket ($ctx, ZMQ::SOCKET_XREP);
        $this->socket->connect ($dsn);
    }
    
    public function connect ($dsn, $ack_dsn)
    {
        $this->socket->connect ($dsn);
        $this->ack_socket->connect ($ack_dsn);
    }
    
    public function consume ($block = true)
    {
        $parts = $this->socket->recvMulti (($block ? 0 : ZMQ::MODE_NOBLOCK));

        if ($parts === false)
            return false;
            
        $message = new PZQMessage ();
        $message->setPeer ($parts [0]);
        $message->setId ($parts [1]);
        $message->setMessage (array_slice ($parts, 3));  

        $this->socket->sendMulti (
                        array (
                            $message->getPeer (), 
                            "", 
                            $message->getId ()
                        )
                    );

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
        
        $data = array ();
        foreach ($parts as $part)
        {
            $pieces = explode (': ', $part);
            $data [$pieces [0]] = $pieces [1];
        }
        
        return $data;
    }
}

