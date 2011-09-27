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
#include "socket.hpp"

int main (int argc, char *argv [])
{
    zmq::context_t context (1);

    pzq::socket_t socket (context, ZMQ_DEALER);
    socket.connect ("tcp://127.0.0.1:11131");

    for (int i = 0; i < 10000; i++)
    {
        pzq::message_t parts;
        boost::shared_ptr<zmq::message_t> id (new zmq::message_t (sizeof (int)));
        memcpy (id.get ()->data (), &i, sizeof (int));
        parts.push_back (id);

        boost::shared_ptr<zmq::message_t> delimiter (new zmq::message_t);
        parts.push_back (delimiter);

        boost::shared_ptr<zmq::message_t> datas (new zmq::message_t (sizeof (int)));
        memcpy (datas.get ()->data (), &i, sizeof (int));
        parts.push_back (datas);

        socket.send_many (parts);

        pzq::message_t reply;
        socket.recv_many (reply);
    }
    return 0;
}