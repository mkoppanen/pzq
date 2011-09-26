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

extern sig_atomic_t keep_running;

bool pzq::manager_t::send_ack (boost::shared_ptr<zmq::message_t> peer_id, boost::shared_ptr<zmq::message_t> ticket, const std::string &status)
{
    // The peer identifier
    m_in.get ()->send (*peer_id.get (), ZMQ_SNDMORE);

    // Blank part 
    zmq::message_t blank;
    m_in.get ()->send (blank, ZMQ_SNDMORE);

    // The request id
    m_in.get ()->send (*ticket.get (), ZMQ_SNDMORE);

    // The request status
    zmq::message_t rstatus (status.size ());
    memcpy (rstatus.data (), status.c_str (), status.size ());
    m_in.get ()->send (rstatus, 0);

    return true;
}

void pzq::manager_t::handle_receiver_in ()
{
    pzq_mp_message parts;

    if (m_in.get ()->recv_many (parts) > 0)
    {
        boost::shared_ptr<zmq::message_t> peer_id = parts.front ();
        parts.pop_front ();

        boost::shared_ptr<zmq::message_t> ticket  = parts.front ();
        parts.pop_front ();

        try {
            m_store.get ()->save (parts);
            send_ack (peer_id, ticket, "OK");
        } catch (std::exception &e) {
            std::cerr << "Failed to store message: " << e.what () << std::endl;
            send_ack (peer_id, ticket, "NOT OK");
        }
    }
}

void pzq::manager_t::handle_sender_ack ()
{
    pzq_mp_message parts;

    if (m_out.get ()->recv_many (parts) > 0)
    {
        std::string key (static_cast <char *>(parts.back ().get ()->data ()), parts.back ().get ()->size ());
        try {
            m_store.get ()->remove (key);
        } catch (std::exception &e) {
            std::cerr << "Failed to remove record: " << e.what () << std::endl;
        }
    }
}

void pzq::manager_t::handle_sender_out ()
{
    // TODO: messages in flight limit is hard coded
    if (m_visitor.can_write () && m_store.get ()->messages_in_flight () < 10)
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

void pzq::manager_t::handle_monitor_in ()
{
    pzq_mp_message message;
    if (m_monitor.get ()->recv_many (message) == 3)
    {
        if (message.back ().get ()->size () == strlen ("MONITOR") &&
            !memcmp (message.back ().get ()->data (), "MONITOR", message.back ().get ()->size ()))
        {
            std::stringstream datas;
            datas << "messages: "           << m_store.get ()->messages ()           << std::endl;
            datas << "messages_in_flight: " << m_store.get ()->messages_in_flight () << std::endl;
            datas << "db_size: "            << m_store.get ()->db_size ()            << std::endl;
            datas << "in_flightdb_size: "   << m_store.get ()->inflight_db_size ()   << std::endl;
            datas << "syncs: "              << m_store.get ()->num_syncs ()          << std::endl;
            datas << "expired_messages: "   << m_store.get ()->num_expired ()        << std::endl;

            m_monitor.get ()->send (*message.front ().get (), ZMQ_SNDMORE);

            zmq::message_t delim;
            m_monitor.get ()->send (delim, ZMQ_SNDMORE);

            std::string blob = datas.str ();

            zmq::message_t message (blob.size ());
            memcpy (message.data (), blob.c_str (), blob.size ());

            m_monitor.get ()->send (message, 0);
        }
    }
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

    // Set time out on store
    m_store.get ()->set_ack_timeout (m_ack_timeout);

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
            // Message coming in from the left side
            handle_receiver_in ();
        }

        if (items [1].revents & ZMQ_POLLIN)
        {
            // ACK coming in from right side
            handle_sender_ack ();
        }

        if (items [1].revents & ZMQ_POLLOUT)
        {
            // Sending messages to right side
            handle_sender_out ();
        }

        if (items [2].revents & ZMQ_POLLIN)
        {
            // Monitoring request
            handle_monitor_in ();
        }
    }
}
