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

#include "receiver.hpp"

extern sig_atomic_t keep_running;

pzq::receiver_t::receiver_t (zmq::context_t &ctx, std::string &database_file, int divisor, uint64_t inflight_size) : m_receive_dsn ("tcp://*:11131"), m_ack_dsn ("tcp://*:11132")
{
	int linger = 1000;

    m_socket.reset (new zmq::socket_t (ctx, ZMQ_XREP));
	m_socket.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));

    m_ack_socket.reset (new zmq::socket_t (ctx, ZMQ_PULL));
	m_ack_socket.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
}

bool pzq::receiver_t::send_response (boost::shared_ptr<zmq::message_t> peer_id, boost::shared_ptr<zmq::message_t> ticket, const std::string &status)
{
	// The peer identifier
	zmq::message_t header (peer_id.get ()->size ());
	memcpy (header.data (), peer_id.get ()->data (), peer_id.get ()->size ());
	m_socket.get ()->send (header, ZMQ_SNDMORE);

	// Blank part 
	zmq::message_t blank;
	m_socket.get ()->send (blank, ZMQ_SNDMORE);

	// The request id
	zmq::message_t rid (ticket.get ()->size ());
	memcpy (rid.data (), ticket.get ()->data (), ticket.get ()->size ());
	m_socket.get ()->send (rid, ZMQ_SNDMORE);

	// The request status
	zmq::message_t rstatus (status.size ());
	memcpy (rstatus.data (), status.c_str (), status.size ());
	m_socket.get ()->send (rstatus, 0);

	return true;
}

void pzq::receiver_t::run ()
{
    std::cerr << "Loaded " << m_store.get ()->messages () << " messages from store" << std::endl;

    // Init sockets
    m_socket.get ()->bind (m_receive_dsn.c_str ());
    m_ack_socket.get ()->bind (m_ack_dsn.c_str ());

    int rc;
    zmq::pollitem_t items [2];
    items [0].socket = *m_socket;
    items [0].fd = 0;
    items [0].events = ZMQ_POLLIN;
    items [0].revents = 0;

    items [1].socket = *m_ack_socket;
    items [1].fd = 0;
    items [1].events = ZMQ_POLLIN;
    items [1].revents = 0;

    while (keep_running) { 
        try {
            int timeout = (m_store.get ()->messages () > 0) ? 10000 : 1000000;
            rc = zmq::poll (&items [0], 2, timeout);
        } catch (std::exception& e) {
            std::cerr << e.what () << ". exiting.." << std::endl;
            break;
        }

        if (rc < 0)
            throw std::runtime_error ("Failed to poll");

        if (items [0].revents & ZMQ_POLLIN)
        {
            int64_t more;
            size_t moresz = sizeof (int64_t);
            std::vector <pzq_message> message_parts;

            do {
                boost::shared_ptr<zmq::message_t> message (new zmq::message_t ());
                m_socket.get ()->recv (message.get (), 0);

                m_socket.get ()->getsockopt (ZMQ_RCVMORE, &more, &moresz);
                int flags = (more) ? ZMQ_SNDMORE : 0;

				if (message.get ()->size () > 0)
                	message_parts.push_back (std::make_pair (message, flags));
			} while (more);

			if (message_parts.size () < 2)
			{
				std::cerr << "The message doesn't contain enough parts" << std::endl;
				continue;
			}

			boost::shared_ptr<zmq::message_t> ticket = message_parts [1].first;
			message_parts.erase (message_parts.begin () + 1);

            try {
                m_store.get ()->save (message_parts);
				send_response (message_parts [0].first, ticket, "OK");
			} catch (std::exception &e) {
                std::cerr << "Failed to store message: " << e.what () << std::endl;
				send_response (message_parts [0].first, ticket, "NOT OK");
            }
        }

        if (items [1].revents & ZMQ_POLLIN)
        {
            // The item to delete
			zmq::message_t message;

			// Delete socket handling
			m_ack_socket.get ()->recv (&message, 0);

			std::string key (static_cast <char *>(message.data ()), message.size ());
			m_store.get ()->remove (key);
		}

        if (m_store.get ()->messages () > 0 && m_sender.get ()->can_write ())
        {
            try {
                m_store.get ()->iterate (m_sender.get ());
            } catch (std::exception &e) {
                std::cerr << "Datastore iteration stopped: " << e.what () << std::endl;
            }
        }
    }
}

