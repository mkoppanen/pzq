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
#include "monitor.hpp"

extern sig_atomic_t keep_running;

pzq::monitor_t::monitor_t (zmq::context_t &ctx, const std::string &dsn, int frequency) : m_frequency (frequency)
{
	int linger = 1000;
    m_socket.reset (new zmq::socket_t (ctx, ZMQ_XREP));
    m_socket.get ()->bind (dsn.c_str ());
	m_socket.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
}

void pzq::monitor_t::run ()
{
    int rc;
    zmq::pollitem_t items [1];
    items [0].socket = *m_socket;
    items [0].fd = 0;
    items [0].events = ZMQ_POLLIN;
    items [0].revents = 0;

    while (keep_running)
    {
        try {
            rc = zmq::poll (&items [0], 1, -1);
        } catch (std::exception& e) {
            continue;
        }

        if (rc < 0)
            throw std::runtime_error ("Failed to poll in monitoring thread");

        if (items [0].revents & ZMQ_POLLIN)
        {
            int64_t more;
            size_t moresz = sizeof (int64_t);

            zmq::message_t id;
            m_socket.get ()->recv (&id, 0);

            do {
                zmq::message_t blank;
                m_socket.get ()->recv (&blank, 0);
                m_socket.get ()->getsockopt (ZMQ_RCVMORE, &more, &moresz);
            } while (more);

            std::stringstream datas;
            datas << "messages: "           << m_store.get ()->messages ()           << std::endl;
            datas << "messages_in_flight: " << m_store.get ()->messages_in_flight () << std::endl;
            datas << "db_size: "            << m_store.get ()->db_size ()            << std::endl;
            datas << "in_flightdb_size: "   << m_store.get ()->inflight_db_size ()   << std::endl;
            datas << "syncs: "              << m_store.get ()->num_syncs ()          << std::endl;

            m_socket.get ()->send (id, ZMQ_SNDMORE);

            zmq::message_t delim;
            m_socket.get ()->send (delim, ZMQ_SNDMORE);

            std::string blob = datas.str ();

            zmq::message_t message (blob.size ());
            memcpy (message.data (), blob.c_str (), blob.size ());

            m_socket.get ()->send (message, 0);
        }
    }
}