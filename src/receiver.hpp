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

#ifndef PZQ_RECEIVER_HPP
# define PZQ_RECEIVER_HPP

#include "pzq.hpp"
#include "store.hpp"
#include "runnable.hpp"
#include "sender.hpp"


using namespace kyotocabinet;

namespace pzq
{
    class receiver_t : public runnable_t
    {
    protected:
        boost::scoped_ptr<zmq::socket_t> m_socket;
        boost::scoped_ptr<zmq::socket_t> m_delete_socket;

        pzq::sender_t    m_sender;
        boost::shared_ptr<pzq::datastore_t> m_store;

		bool send_response (boost::shared_ptr<zmq::message_t> peer_id, boost::shared_ptr<zmq::message_t> ticket, const std::string &status);

    public:
        receiver_t (zmq::context_t &ctx, std::string &database_file, int divisor, uint64_t inflight_size);

        void run ();
    };
}

#endif
