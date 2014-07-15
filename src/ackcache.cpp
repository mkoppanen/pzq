/*
 *  Copyright 2014 Cédric Pessan <cedric.pessan@gmail.com>
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

#include "ackcache.hpp"
#include "time.hpp"

using std::string;

namespace pzq
{
    ackcache_t::ackcache_t( uint64_t timeoutReplication )
    {
        pzq::log( "Initializing ack cache" );
        m_timeoutReplication = timeoutReplication;
    }
    
    ackcache_t::~ackcache_t()
    {
    }
    
    void ackcache_t::push( const std::string& idMsg, pzq::message_t ack, int replicas )
    {
        m_cache.insert( ack_t( idMsg, ack, microsecond_timestamp() + m_timeoutReplication, replicas ) );
    }
    
    pzq::message_t ackcache_t::getAndRemoveById( string id )
    {
        pzq::message_t msg;
        
        cache_t::nth_index< 0 >::type& index = m_cache.get< 0 >();
        cache_t::nth_index< 0 >::type::iterator it = index.find( id );
        if( it != index.end() )
        {
            it->decrementReplicas();
            if(it->getReplicas() == 0 )
            {
                msg = it->getAck();
                index.erase( it );
            }
        }
        
        return msg;
    }
    
    int ackcache_t::getDelayUntilNextAck() const
    {
        if( m_cache.size() == 0) return std::numeric_limits< int >::max();
        
        const cache_t::nth_index< 1 >::type& index = m_cache.get< 1 >();
        return ( (int64_t)microsecond_timestamp() - (int64_t)index.begin()->m_ts ) / 1000;
    }
    
    pzq::message_t ackcache_t::pop()
    {
        if( m_cache.size() == 0) return pzq::message_t();
        
        cache_t::nth_index< 1 >::type& index = m_cache.get< 1 >();
        pzq::message_t msg = index.begin()->getAck();
        index.erase( index.begin() );
        return msg;
    }
    
    ackcache_t::ack_t::ack_t( const string& idmsg, pzq::message_t ack, uint64_t ts, int replicas )
    {
        m_idmsg = idmsg;
        m_ts= ts;
        m_ack = ack;
        m_replicas = shared_ptr< int >( new int );
        *m_replicas = replicas;
    }
    
    ackcache_t::ack_t::~ack_t()
    {
    }
    
    ackcache_t::ack_t::ack_t( const ack_t& ack )
    {
        m_idmsg = ack.m_idmsg;
        m_ts = ack.m_ts;
    }
    
    pzq::message_t ackcache_t::ack_t::getAck() const
    {
        return m_ack;
    }
    
    void ackcache_t::ack_t::decrementReplicas() const
    {
        (*m_replicas)--;
    }
    
    int ackcache_t::ack_t::getReplicas() const
    {
        return *m_replicas;
    }
}
