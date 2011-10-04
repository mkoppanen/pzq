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

        // peer id
        ack.append (parts.pop_front ());

        // message id
        ack.append (parts.pop_front ());

        // TODO: this only ignores the empty part, should probably
        // ignore everything before the empty part.
        parts.pop_front ();

        bool success;
        std::string status_message;

        try {
            m_store.get ()->save (parts);
            success = true;
        } catch (std::exception &e) {
            success = false;
            status_message = e.what ();
        }

        // Status code
        ack.append ((void *)(success ? "1" : "0"), 1);

        // delimiter
        ack.append ();

        if (!success && status_message.size ())
            ack.append (status_message);

        m_in->send_many (ack);
    }
}

void pzq::manager_t::handle_consumer_ack ()
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
                m_store.get ()->remove (key);
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
    } catch (std::exception &e) {
#if 1
        std::cerr << "Datastore iteration stopped: " << e.what () << std::endl;
#endif
    }
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

    while (is_running ())
    {
        items [1].events = ((m_store.get ()->messages_pending ()) ? (ZMQ_POLLIN | ZMQ_POLLOUT) : ZMQ_POLLIN);

        try {
            rc = zmq::poll (&items [0], 3, -1);
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
            handle_consumer_ack ();
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
    }
}
