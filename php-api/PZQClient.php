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
    private $id = null;
             
    private $peer = null;
             
    private $message = null;
             
    private $sent = null;
             
    private $ack_timeout = null;

    public function get_id ()
    {
        return $this->id;
    }
    
    public function set_id ($id)
    {
        return $this->id = $id;
    }
    
    public function get_sent_time ()
    {
        return $this->sent;
    }
    
    public function set_sent_time ($sent)
    {
        $this->sent = $sent;
    }    
    
    public function get_ack_timeout ()
    {
        return $this->ack_timeout;
    }
    
    public function set_ack_timeout ($timeout)
    {
        $this->ack_timeout = $timeout;
    }

    public function get_peer ()
    {
        return $this->peer;
    }
    
    public function set_peer ($peer)
    {
        return $this->peer = $peer;
    }    
    
    public function get_message ()
    {
        return $this->message;
    }
    
    public function set_message ($message)
    {
        $this->message = $message;
    }
}

class PZQProducer 
{
    private $socket;
    
    private $poll;
    
    public function __construct ($dsn = null)
    {
        $this->socket = new ZMQSocket (new ZMQContext (), ZMQ::SOCKET_XREQ);
        
        if ($dsn)
        {
            $this->connect ($dsn);
        }
        
        $this->poll = new ZMQPoll ();
        $this->poll->add ($this->socket, ZMQ::POLL_IN);
    }
    
    public function connect ($dsn)
    {
        $this->socket->connect ($dsn);
    }
    
    public function produce (PZQMessage $message, $timeout = 5000)
    {
        $out = array ($message->get_id (), "");
        $m = $message->get_message ();
        
        if (is_array ($m))
            $out = array_merge ($out, $m);
        else
            array_push ($out, $m);
        
        $this->socket->sendMulti ($out);
        
        $r = $w = array ();
        $this->poll->poll ($r, $w, $timeout);
        
        if (empty ($r))
            throw new PZQClientException ('ACK timeout');
        
        $response = $this->socket->recvMulti ();
        
        if ($response [1] != $message->get_id ()) 
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
    
    private $timeout;
    
    private $filter_expired = true;

    public function __construct ($dsn = null)
    {
        $ctx = new ZMQContext ();
        $this->socket = new ZMQSocket ($ctx, ZMQ::SOCKET_XREP);
        
        if ($dsn)
            $this->connect ($dsn);
    }
    
    public function set_filter_expired ($value)
    {
        $this->filter_expired = $value;
    }

    public function connect ($dsn)
    {
        $this->socket->connect ($dsn);
    }
    
    public function consume ($block = true)
    {
        $parts = $this->socket->recvMulti (($block ? 0 : ZMQ::MODE_NOBLOCK));

        if ($parts === false)
            return false;

        $message = new PZQMessage ();
        $message->set_peer ($parts [0]);
        $message->set_id ($parts [1]);
        $message->set_sent_time ($parts [2]);
        $message->set_ack_timeout ($parts [3]);
        $message->set_message (array_slice ($parts, 5));

        if ($this->filter_expired && $this->is_expired ($message)) {
            return $this->consume ($block);
        }
        return $message;
    }
    
    public function is_expired (PZQMessage $message)
    {
        $t    = microtime (true);
        $sent = (float) ($message->get_sent_time () / 1000000);
        $diff = $t - $sent;

        return ($diff > ($message->get_ack_timeout ()/ 1000000));
    }
    
    public function ack (PZQMessage $message)
    {
        $this->socket->sendMulti (
                        array (
                            $message->get_peer (), 
                            "", 
                            $message->get_id ()
                        )
                    );
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

