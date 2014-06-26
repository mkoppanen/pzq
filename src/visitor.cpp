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
#include "visitor.hpp"
#include "time.hpp"

bool pzq::visitor_t::can_write ()
{
    int events = 0;
    size_t optsiz = sizeof (int);

    m_socket.get ()->getsockopt (ZMQ_EVENTS, &events, &optsiz);

    if (events & ZMQ_POLLOUT)
        return true;

    return false;
}

const char *pzq::visitor_t::visit_full (const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz, size_t *sp) 
{
    size_t msg_size, pos = 0;
	std::string key (kbuf, ksiz);

	if (!can_write ())
        throw std::runtime_error ("Reached maximum messages in flight limit");

	if ((*m_store).is_in_flight (key))
		return NOP;

    pzq::message_t parts;
    parts.append (key);
   
    // Check if it is a replica and if it should be sent
    memcpy( &msg_size, vbuf, sizeof( uint64_t ) );
    if( msg_size > 8 && strncmp( vbuf+sizeof(uint64_t), "REPLICA:", 8 ) == 0 )
     {
	std::string replicaSource = std::string( vbuf+sizeof(uint64_t) + 8, msg_size - 8 );
	
	if( !m_cluster->shouldSendReplica( replicaSource ) )
	  return NOP;
	
	m_cluster->checkReplica( key, replicaSource );
     }

    // Time when the message goes out
    std::stringstream mt;
    mt << pzq::microsecond_timestamp ();

    parts.append (mt.str ());

    std::stringstream expiry;
    expiry << (*m_store).get_ack_timeout ();

    parts.append (expiry.str ());
    parts.append ();
    while (true)
    {
        memcpy (&msg_size, vbuf + pos, sizeof (uint64_t));
        pos += sizeof (uint64_t);

        parts.append (vbuf + pos, msg_size);

        pos += msg_size;
        if (pos >= vsiz)
            break;
    }
   
    if ((*m_socket).send_many (parts, ZMQ_NOBLOCK))
        (*m_store).mark_in_flight (key);
    else
        throw std::runtime_error ("Reached maximum messages in flight limit");
   
    return NOP;
}
