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

void pzq::manager_t::handle_receiver_in ()
{
    pzq::message_t parts;

    if (m_in.get ()->recv_many (parts) > 0)
    {
        pzq::message_t ack;

        // peer id 
        ack.push_back (parts.front ());
        parts.pop_front ();

        // message id
        ack.push_back (parts.front ());
        parts.pop_front ();

        try {
            m_store.get ()->save (parts);

            boost::shared_ptr<zmq::message_t> part (new zmq::message_t (2));
            memcpy (part.get ()->data (), "OK", 2);
            ack.push_back (part);

        } catch (std::exception &e) {
            boost::shared_ptr<zmq::message_t> part (new zmq::message_t (2));
            memcpy (part.get ()->data (), "NO", 2);
            ack.push_back (part);
        }
        m_in->send_many (ack);
    }
}

void pzq::manager_t::handle_sender_ack ()
{
    pzq::message_t parts;

    if (m_out.get ()->recv_many (parts) > 0)
    {
        static int got_ack = 0;
        got_ack++;
        std::cerr << "Got ACK: " << got_ack << std::endl;
        
        
        /*
        std::string key (static_cast <char *>(parts.back ().get ()->data ()), parts.back ().get ()->size ());
        try {
            m_store.get ()->remove (key);
        } catch (std::exception &e) {
            std::cerr << "Not removing record (" << key << "): " << e.what () << std::endl;
        }*/
    }
}

void pzq::manager_t::handle_sender_out ()
{
    try {
        while (true)
        {
            std::string key ("hello", 5);

            pzq::message_t parts;

            boost::shared_ptr<zmq::message_t> header (new zmq::message_t (key.size ()));
            memcpy (header.get ()->data (), key.c_str (), key.size ());
            parts.push_back (header);

            // Time when the message goes out
            std::stringstream mt;
            mt << pzq::microsecond_timestamp ();

            boost::shared_ptr<zmq::message_t> out_time (new zmq::message_t (mt.str ().size ()));
            memcpy (out_time.get ()->data (), mt.str ().c_str (), mt.str ().size ());
            parts.push_back (out_time);

            std::stringstream expiry;
            expiry << m_store.get ()->get_ack_timeout ();

            boost::shared_ptr<zmq::message_t> ack_timeout (new zmq::message_t (expiry.str ().size ()));
            memcpy (ack_timeout.get ()->data (), expiry.str ().c_str (), expiry.str ().size ());
            parts.push_back (ack_timeout);

            if (!m_out.get ()->send_many (parts, ZMQ_NOBLOCK))
                throw std::runtime_error ("Cannot send");
        }
        //m_store.get ()->iterate (&m_visitor);
    } catch (std::exception &e) {
#ifdef DEBUG
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
            datas << "messages_in_flight: " << m_store.get ()->messages_in_flight ()   << std::endl;
            datas << "db_size: "            << m_store.get ()->db_size ()              << std::endl;
            datas << "in_flightdb_size: "   << m_store.get ()->inflight_db_size ()     << std::endl;
            datas << "syncs: "              << m_store.get ()->num_syncs ()            << std::endl;
            datas << "expired_messages: "   << m_store.get ()->get_messages_expired () << std::endl;

            pzq::message_t reply;
            reply.push_back (message.front ());

            boost::shared_ptr<zmq::message_t> delimiter (new zmq::message_t);
            reply.push_back (delimiter);

            std::string blob = datas.str ();

            boost::shared_ptr<zmq::message_t> values (new zmq::message_t (blob.size ()));
            memcpy (values.get ()->data (), blob.c_str (), blob.size ());
            reply.push_back (values);

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
    items [1].events  = ZMQ_POLLIN | ZMQ_POLLOUT;
    items [1].revents = 0;

    items [2].socket  = *m_monitor;
    items [2].fd      = 0;
    items [2].events  = ZMQ_POLLIN;
    items [2].revents = 0;

    // Set time out on store
    m_store.get ()->set_ack_timeout (m_ack_timeout);

    while (is_running ())
    {
        try {
            rc = zmq::poll (&items [0], 2, -1);
        } catch (std::exception& e) {
            std::cerr << e.what () << ". exiting.." << std::endl;
            break;
        }

        if (rc < 0)
            throw std::runtime_error ("Poll failed");

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
    }
}
