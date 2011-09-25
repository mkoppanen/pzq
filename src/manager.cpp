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

extern sig_atomic_t keep_running;

bool pzq::manager_t::send_ack (boost::shared_ptr<zmq::message_t> peer_id, boost::shared_ptr<zmq::message_t> ticket, const std::string &status)
{
    // The peer identifier
    zmq::message_t header (peer_id.get ()->size ());
    memcpy (header.data (), peer_id.get ()->data (), peer_id.get ()->size ());
    m_in.get ()->send (header, ZMQ_SNDMORE);

    // Blank part 
    zmq::message_t blank;
    m_in.get ()->send (blank, ZMQ_SNDMORE);

    // The request id
    zmq::message_t rid (ticket.get ()->size ());
    memcpy (rid.data (), ticket.get ()->data (), ticket.get ()->size ());
    m_in.get ()->send (rid, ZMQ_SNDMORE);

    // The request status
    zmq::message_t rstatus (status.size ());
    memcpy (rstatus.data (), status.c_str (), status.size ());
    m_in.get ()->send (rstatus, 0);

    return true;
}

void pzq::manager_t::run ()
{
    int rc;
    zmq::pollitem_t items [3];
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

    while (keep_running) {
        bool messages_pending = m_store.get ()->messages_pending ();

        if (messages_pending)
            items [1].events = ZMQ_POLLIN | ZMQ_POLLOUT;
        else
            items [1].events  = ZMQ_POLLIN;

        try {
            rc = zmq::poll (&items [0], 3, (messages_pending ? 100000 : -1));
        } catch (std::exception& e) {
            std::cerr << e.what () << ". exiting.." << std::endl;
            break;
        }

        if (items [0].revents & ZMQ_POLLIN)
        {
            int64_t more;
            size_t moresz = sizeof (int64_t);
            std::vector <pzq_message> message_parts;

            do {
                boost::shared_ptr<zmq::message_t> message (new zmq::message_t ());
                m_in.get ()->recv (message.get (), 0);

                m_in.get ()->getsockopt (ZMQ_RCVMORE, &more, &moresz);
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
                send_ack (message_parts [0].first, ticket, "OK");
            } catch (std::exception &e) {
                std::cerr << "Failed to store message: " << e.what () << std::endl;
                send_ack (message_parts [0].first, ticket, "NOT OK");
            }
        }

        if (items [1].revents & ZMQ_POLLIN)
        {
            int64_t more;
            size_t moresz = sizeof (int64_t);

            // The item to delete
            zmq::message_t message;

            // Delete socket handling
            m_out.get ()->recv (&message, 0);
            m_out.get ()->getsockopt (ZMQ_RCVMORE, &more, &moresz);

            if (!more)
            {
                std::string key (static_cast <char *>(message.data ()), message.size ());
                try {
                    m_store.get ()->remove (key);
                } catch (std::exception &e) {
                    std::cerr << "Failed to remove record: " << e.what () << std::endl;
                }
            }
        }

        if (items [1].revents & ZMQ_POLLOUT)
        {
            // TODO: messages in flight limit is hard coded
            if (messages_pending && m_visitor.can_write () && m_store.get ()->messages_in_flight () < 10)
            {
                try {
                    m_store.get ()->iterate (&m_visitor);
                } catch (std::exception &e) {
#ifdef DEBUG
                    std::cerr << "Datastore iteration stopped: " << e.what () << std::endl;
#endif
                }
            }
        }

        if (items [2].revents & ZMQ_POLLIN)
        {
            int64_t more;
            size_t moresz = sizeof (int64_t);

            zmq::message_t id;
            m_monitor.get ()->recv (&id, ZMQ_NOBLOCK);

            m_monitor.get ()->getsockopt (ZMQ_RCVMORE, &more, &moresz);
            if (!more)
                continue;

            zmq::message_t blank;
            m_monitor.get ()->recv (&blank, ZMQ_NOBLOCK);

            m_monitor.get ()->getsockopt (ZMQ_RCVMORE, &more, &moresz);
            if (!more)
                continue;

            zmq::message_t command;
            m_monitor.get ()->recv (&command, ZMQ_NOBLOCK);

            if (command.size () == strlen ("MONITOR") &&
                !memcmp (command.data (), "MONITOR", command.size ()))
            {
                std::stringstream datas;
                datas << "messages: "           << m_store.get ()->messages ()           << std::endl;
                datas << "messages_in_flight: " << m_store.get ()->messages_in_flight () << std::endl;
                datas << "db_size: "            << m_store.get ()->db_size ()            << std::endl;
                datas << "in_flightdb_size: "   << m_store.get ()->inflight_db_size ()   << std::endl;
                datas << "syncs: "              << m_store.get ()->num_syncs ()          << std::endl;
                datas << "expired_messages: "   << m_store.get ()->num_expired ()        << std::endl;

                m_monitor.get ()->send (id, ZMQ_SNDMORE);

                zmq::message_t delim;
                m_monitor.get ()->send (delim, ZMQ_SNDMORE);

                std::string blob = datas.str ();

                zmq::message_t message (blob.size ());
                memcpy (message.data (), blob.c_str (), blob.size ());

                m_monitor.get ()->send (message, 0);
            }
        }
    }
}