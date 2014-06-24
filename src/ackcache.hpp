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

#ifndef PZQ_ACKCACHE_HPP
#define PZQ_ACKCACHE_HPP

#include "pzq.hpp"

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

using namespace ::boost;
using namespace ::boost::multi_index;

namespace pzq
{
   class ackcache_t
     {
      public:
	ackcache_t( uint64_t timeoutReplication );
	~ackcache_t();
	
	void push( const std::string& idMsg, pzq::message_t ack, int replicas );
	
	pzq::message_t getAndRemoveById( std::string id );
	
	int getDelayUntilNextAck() const;
	
	pzq::message_t pop();
	
	class ack_t
	  {
	   public:
	     ack_t( const std::string& idMsg, pzq::message_t ack, uint64_t ts, int replicas );
	     ack_t( const ack_t& );
	     ~ack_t();
	     
	     pzq::message_t getAck() const;
	     void decrementReplicas() const;
	     int getReplicas() const;
	     
	   private:
	     ack_t();
	     ack_t& operator=( const ack_t& );
	     
	     pzq::message_t m_ack;
	     
	   public:
	     std::string m_idmsg;
	     uint64_t m_ts;
	     shared_ptr< int > m_replicas;
	  };
	
	typedef multi_index_container<
	  ack_t,
	  indexed_by<
	  hashed_unique< member< ack_t, std::string, &ack_t::m_idmsg > >,
	  ordered_unique< member< ack_t, uint64_t, &ack_t::m_ts > > > > cache_t;
	
      private:
        ackcache_t();
	ackcache_t( const ackcache_t& );
	ackcache_t& operator=( const ackcache_t& );
	
	cache_t  m_cache;
	uint64_t m_timeoutReplication;
     };
}

#endif // PZQ_ACKCACHE_HPP
