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

#ifndef PZQ_MANAGER_HPP
# define PZQ_MANAGER_HPP

#include "pzq.hpp"
#include "store.hpp"
#include "socket.hpp"
#include "visitor.hpp"
#include "cluster.hpp"
#include "ackcache.hpp"

using namespace kyotocabinet;

namespace pzq {

    class manager_t : public thread_t
    {
    private:
        boost::shared_ptr<pzq::socket_t> m_in;
        boost::shared_ptr<pzq::socket_t> m_out;
        boost::shared_ptr<pzq::socket_t> m_monitor;
        boost::shared_ptr<pzq::datastore_t> m_store;
        boost::shared_ptr<pzq::cluster_t > m_cluster;
        boost::shared_ptr<pzq::ackcache_t > m_waitingAcks;
        pzq::visitor_t m_visitor;
        uint64_t m_ack_timeout;
        boost::mutex m_mutex;

        void handle_producer_in ();

        void handle_consumer_in ();

        void handle_consumer_out ();

        void handle_monitor_in ();

        bool send_ack (boost::shared_ptr<zmq::message_t> peer_id, boost::shared_ptr<zmq::message_t> ticket, const std::string &status);

    public:
        void set_sockets (boost::shared_ptr<pzq::socket_t> in, boost::shared_ptr<pzq::socket_t> out, boost::shared_ptr<pzq::socket_t> monitor, boost::shared_ptr<pzq::cluster_t> cluster)
        {
            m_mutex.lock ();
            m_in = in;
            m_out = out;
            m_monitor = monitor;
            m_visitor.set_socket (m_out, cluster);
            m_mutex.unlock ();
        }

        void set_ack_timeout (uint64_t ack_timeout)
        {
            m_ack_timeout = ack_timeout;
        }

        void set_datastore (boost::shared_ptr<pzq::datastore_t> store)
		{
			m_store = store;
			m_visitor.set_datastore (store);
		}
       
        void set_cluster( boost::shared_ptr< pzq::cluster_t > cluster )
	 {
	    m_cluster = cluster;
	 }
       
        void set_ack_cache( boost::shared_ptr< pzq::ackcache_t > ackCache )
	 {
	    m_waitingAcks = ackCache;
	 }

        void run ();
    };
}

#endif
