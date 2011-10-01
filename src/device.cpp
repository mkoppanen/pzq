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
#include "device.hpp"

void pzq::device_t::device ()
{
    zmq_pollitem_t items [2];
    items [0].socket = *m_in;
    items [0].fd = 0;
    items [0].events = ZMQ_POLLIN;
    items [0].revents = 0;

    items [1].socket = *m_out;
    items [1].fd = 0;
    items [1].events = ZMQ_POLLIN;
    items [1].revents = 0;

    int rc;
    int in = 0, out = 0;

    while (true) {
        //  Wait while there are either requests or replies to process.
        rc = zmq_poll (&items [0], 2, -1);
        if (rc < 0) {
            return;
        }

        //  Process a request.
        if (items [0].revents & ZMQ_POLLIN) {     
            pzq::message_t parts;
            if (m_in.get ()->recv_many (parts) > 0)
            {
                m_out.get ()->send_many (parts);
                std::cerr << "Message from " << m_name << " in to out: " << in << std::endl;
                in++;
            }
        }

        //  Process a reply.
        if (items [1].revents & ZMQ_POLLIN) {
            pzq::message_t parts;
            if (m_out.get ()->recv_many (parts) > 0)
            {
                m_in.get ()->send_many (parts);
                std::cerr << "Message from " << m_name << " out to in: " << out << std::endl;
                out++;
            }
        }
    }
}

void pzq::device_t::run ()
{
    try {
        device ();
    } catch (zmq::error_t &e) {
        std::cerr << e.what () << std::endl;
    }
}

