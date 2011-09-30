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

    pzq::socket_t socket (context, ZMQ_ROUTER);
    socket.connect ("tcp://127.0.0.1:11132");

    for (int i = 0; i < 1000; i++)
    {
        pzq::message_t parts;
        if (socket.recv_many (parts)) {

            pzq::message_t reply;

            reply.push_back (parts.front ());
            parts.pop_front ();

            boost::shared_ptr<zmq::message_t> delimiter (new zmq::message_t);
            reply.push_back (delimiter);

            reply.push_back (parts.front ());
            socket.send_many (reply);
        }
    }
    return 0;
}