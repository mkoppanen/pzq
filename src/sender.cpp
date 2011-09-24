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

#include "sender.hpp"

pzq::sender_t::sender_t (zmq::context_t &ctx, std::string &dsn, bool use_pub)
{
    uint64_t hwm = 100;
	int linger = 1000;
    int type = (use_pub) ? ZMQ_PUB : ZMQ_PUSH;

    m_socket.reset (new zmq::socket_t (ctx, type));
    m_socket.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
    m_socket.get ()->bind (dsn.c_str ());
	m_socket.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
}

bool pzq::sender_t::can_write ()
{
    int events = 0;
    size_t optsiz = sizeof (int);

    m_socket.get ()->getsockopt (ZMQ_EVENTS, &events, &optsiz);

    if (events & ZMQ_POLLOUT)
        return true;

    return false;
}

const char *pzq::sender_t::visit_full (const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz, size_t *sp) 
{
    size_t pos = 0;
    int32_t flags;
    size_t msg_size;
    bool more = true;

	std::string key (kbuf, ksiz);

	if (m_store.get ()->is_in_flight (key))
	{
		return NOP;
	}

    if (!can_write ())
    	throw std::runtime_error ("The socket is in blocking state");

    zmq::message_t header (ksiz);
    memcpy (header.data (), kbuf, ksiz);

    if (!m_socket.get ()->send (header, ZMQ_SNDMORE))
        throw std::runtime_error ("Failed to send the message header");

    while (more)
    {
        memcpy (&flags, vbuf + pos, sizeof (int32_t));
        pos += sizeof (int32_t);

        int tmp_flags = static_cast <int> (flags);

        memcpy (&msg_size, vbuf + pos, sizeof (size_t));
        pos += sizeof (size_t);

        zmq::message_t msg (msg_size);
        memcpy (msg.data (), vbuf + pos, msg_size);
        pos += msg_size;

        if (!can_write ())
        	throw std::runtime_error ("The socket is in blocking state");

		if (!m_socket.get ()->send (msg, tmp_flags))
        	throw std::runtime_error ("Failed to send the message part");

        more = (flags & ZMQ_SNDMORE);
    }
	m_store->mark_in_flight (key);
    return NOP;
}

const char *pzq::sender_t::visit_empty (const char* kbuf, size_t ksiz, size_t* sp) 
{
    return NOP;
}
