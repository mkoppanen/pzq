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
#include "visitor.hpp"

using namespace kyotocabinet;

namespace pzq {

    class manager_t
    {
    private:
        boost::shared_ptr<zmq::socket_t> m_in;
        boost::shared_ptr<zmq::socket_t> m_out;
        boost::shared_ptr<pzq::datastore_t> m_store;
        pzq::visitor_t m_visitor;

    public:
        void set_sockets (boost::shared_ptr<zmq::socket_t> in, boost::shared_ptr<zmq::socket_t> out)
        {
            m_in = in;
            m_out = out;
            m_visitor.set_socket (m_out);
        }

        void set_datastore (boost::shared_ptr<pzq::datastore_t> store)
		{
			m_store = store;
			m_visitor.set_datastore (store);
		}

        void start ()
        {
            run ();
        }

        void run ();

        bool send_ack (boost::shared_ptr<zmq::message_t> peer_id, boost::shared_ptr<zmq::message_t> ticket, const std::string &status);
    };
}

#endif