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

#include "pzq.hpp"
#include "manager.hpp"
#include "socket.hpp"

void pzq::manager_t::handle_producer_in ()
{
    pzq::message_t parts;

    if (m_in.get ()->recv_many (parts) > 2)
    {
        pzq::message_t ack;
        pzq::message_t replica = m_cluster->createReplica( parts );
        bool isAReplica = false;
        pzq::message_t idReplica;
        std::string storedKey;

        // peer id
        ack.append (parts.pop_front ());

        // message id
        std::string msgId = std::string( ( char* )parts.front()->data(), parts.front()->size() );
        ack.append (parts.pop_front ());

        while (parts.size () > 0 && parts.front ().get ()->size () > 0)
	 {
	    pzq::message_part_t part = parts.front();
	    std::string header_msg = std::string( ( char* )part->data(), part->size() );
	    const std::string keyword = "REPLICA:";
	    if( header_msg.find( keyword ) == 0 )
	      {
		 isAReplica = true;
		 idReplica.append( part );
	      }
            parts.pop_front ();
	 }

         bool success;
         std::string status_message;

        if (parts.size () == 0)
        {
            success = false;
            status_message = "Malformed message, no delimiter found or missing message parts";
        }
        else
        {
            parts.pop_front ();
	   
	    if( isAReplica )
	     {
		for( message_iterator_t it = parts.begin(); it != parts.end(); ++it )
		  idReplica.append( *it );
		parts = idReplica;
	     }

            try {
                m_store.get ()->save (parts, isAReplica ? msgId : "", storedKey );
                success = true;
            } catch (std::exception &e) {
                success = false;
                status_message = e.what ();
            }
        }

        // Status code
        ack.append ((void *) (success ? "1" : "0"), 1);

        // delimiter
        ack.append ();

        if (!success && status_message.size ())
            ack.append (status_message);
       
        if( success && m_cluster->replicas() > 0 && !isAReplica)
	 {
	    int replicas = m_cluster->replicas();
	    int nodes = m_cluster->countActiveNodes();
	    
	    if( replicas > nodes )
	      {
		 replicas = nodes;
		 pzq::log ("CRITICAL: Could not create %d replicas, only %d nodes in cluster", m_cluster->replicas(), nodes );
	      }
	    
	    if( replicas > 0 )
	      {
                 pzq::message_t replicaWithId;
                 replicaWithId.append( storedKey );
                 message_iterator_t it = replica.begin();
                 ++it;
                 while( it != replica.end() )
                   {
                      replicaWithId.append( *it );
                      it++;
                   }
                 
		 for(int i = 0; i < replicas; i++)
		   m_cluster->getOutSocket()->send_many( replicaWithId );
		 
		 m_waitingAcks->push( storedKey, ack, replicas );
	      }
	    else
	      m_in->send_many( ack );
	 }
       else
	 m_in->send_many (ack);
    }
}

void pzq::manager_t::handle_consumer_in ()
{
    pzq::message_t parts;

    if (m_out.get ()->recv_many (parts) >= 2)
    {
        // The next part is the key
        std::string key;
        parts.front (key);
        parts.pop_front ();

        // The last part indicates whether this was success or fail
        std::string status;
        parts.front (status);

        try {
            if (!status.compare ("1"))
             {
                m_store.get ()->remove (key);
                m_cluster->broadcastRemove( key );
             }
            else
                m_store.get ()->remove_inflight (key);
        } catch (std::exception &e) {
            pzq::log ("Not removing record (%s): %s", e.what ());
        }
    }
}

void pzq::manager_t::handle_consumer_out ()
{
    try {
        m_store.get ()->iterate (&m_visitor);
    } catch (std::exception &e) { }
}

void pzq::manager_t::handle_monitor_in ()
{
    pzq::message_t message;
    if (m_monitor.get ()->recv_many (message) > 0)
    {
        std::string command (static_cast <char *>(message.back ().get ()->data ()),
                             message.back ().get ()->size ());

        if (!command.compare ("MONITOR"))
        {
            std::stringstream datas;
            datas << "messages: "           << m_store.get ()->messages ()             << std::endl;
            datas << "messages_inflight: "  << m_store.get ()->messages_inflight ()    << std::endl;
            datas << "db_size: "            << m_store.get ()->db_size ()              << std::endl;
            datas << "inflight_db_size: "   << m_store.get ()->inflight_db_size ()     << std::endl;
            datas << "syncs: "              << m_store.get ()->num_syncs ()            << std::endl;
            datas << "expired_messages: "   << m_store.get ()->get_messages_expired () << std::endl;

            pzq::message_t reply;
            reply.append (message.front ());

            boost::shared_ptr<zmq::message_t> delimiter (new zmq::message_t);
            reply.append (delimiter);
            reply.append (datas.str ());

            m_monitor.get ()->send_many (reply, 0);
        }
    }
}

void pzq::manager_t::run ()
{
    int rc;
    zmq::pollitem_t items [5];
    items [0].socket  = *m_in;
    items [0].fd      = 0;
    items [0].events  = ZMQ_POLLIN;
    items [0].revents = 0;

    items [1].socket  = *m_out;
    items [1].fd      = 0;
    items [1].events  = ZMQ_POLLIN;
    items [1].revents = 0;

    items [2].socket  = *m_monitor;
    items [2].fd      = 0;
    items [2].events  = ZMQ_POLLIN;
    items [2].revents = 0;
   
    items [3].socket  = *m_cluster->getOutSocket();
    items [3].fd      = 0;
    items [3].events  = ZMQ_POLLIN;
    items [3].revents = 0;
   
    items [4].socket  = *m_cluster->getSubSocket();
    items [4].fd      = 0;
    items [4].events  = ZMQ_POLLIN;
    items [4].revents = 0;

    while (is_running ())
    {
        items [1].events = ((m_store.get ()->messages_pending ()) ? (ZMQ_POLLIN | ZMQ_POLLOUT) : ZMQ_POLLIN);

        try {
	    int pollTimeout = 50000/1000;
	    int ackDelay = m_waitingAcks->getDelayUntilNextAck();
            int nextBroadcast = m_cluster->getDelayUntilNextBroadcast();
            int nextNodeTimeout = m_cluster->getDelayUntilNextNodeTimeout();
	    pollTimeout = ackDelay < pollTimeout ? ackDelay : pollTimeout;
            pollTimeout = nextBroadcast <  pollTimeout ? nextBroadcast : pollTimeout;
            pollTimeout = nextNodeTimeout < pollTimeout ? nextNodeTimeout : pollTimeout;
	    pollTimeout = pollTimeout < 0 ? 0 : pollTimeout;
	     
            rc = zmq::poll (&items [0], 5, pollTimeout );
        } catch (zmq::error_t &e) {
            pzq::log ("Poll interrupted");
            break;
        }

        if (rc < 0)
            throw new std::runtime_error ("zmq::poll failed");

        if (items [0].revents & ZMQ_POLLIN)
        {
            // Message coming in from the left side
            handle_producer_in ();
        }

        if (items [1].revents & ZMQ_POLLIN)
        {
            // ACK coming in from right side
            handle_consumer_in ();
        }

        if (items [1].revents & ZMQ_POLLOUT)
        {
            // Sending messages to right side
            handle_consumer_out ();
        }

        if (items [2].revents & ZMQ_POLLIN)
        {
            // Monitoring request
            handle_monitor_in ();
        }
       
        if (items [3].revents & ZMQ_POLLIN)
	{
	    // ACK coming from other nodes for replicas
	    m_cluster->handleAck( m_in, m_waitingAcks );
	}
       
        if (items [4].revents & ZMQ_POLLIN)
        {
            // Received message from other nodes on subscribe socket
            m_cluster->handleNodesMessage();
        }
       
        while( m_waitingAcks->getDelayUntilNextAck() < 0 )
        {
            // send ack with a message to inform that replication failed, 
            // producer should decide between considering the message as sent or not
	    pzq::message_t ack = m_waitingAcks->pop();
	    ack.append( "REPLICATION_FAILED" );
	    m_in->send_many( ack );
        }
       
        if( m_cluster->getDelayUntilNextBroadcast() < 0 )
        {
            // broadcast a keepalive message to all nodes of the cluster
            m_cluster->broadcastKeepAlive();
        }
       
        if( m_cluster->getDelayUntilNextNodeTimeout() < 0 )
        {
            // if a node expires, we move store cursor to the beginning
            m_cluster->setTimeoutState();
            m_store->resetIterator();
        }
    }
}
