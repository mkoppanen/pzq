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

#ifndef PZQ_CLUSTER_HPP
#define PZQ_CLUSTER_HPP

#include <boost/shared_ptr.hpp>

#include "socket.hpp"
#include "ackcache.hpp"
#include "store.hpp"

namespace pzq
{
   typedef std::map< std::string, uint64_t > nodelist_t;
   
   class cluster_t
     {
      public:
	cluster_t( int replicas, const std::vector< std::string >& nodeNames, uint64_t timeoutNode,
		   boost::shared_ptr< pzq::socket_t > clusterSocket,
                   boost::shared_ptr< pzq::socket_t > broadcastSocket,
                   boost::shared_ptr< pzq::socket_t > subscribeSocket,
		   std::string currentNode,
                   boost::shared_ptr< pzq::datastore_t > store );
	~cluster_t();
	
	int replicas() const;
	int countActiveNodes() const;
	
	boost::shared_ptr< pzq::socket_t > getOutSocket();
        boost::shared_ptr< pzq::socket_t > getSubSocket();
	
	pzq::message_t createReplica( pzq::message_t orig ) const;
	
	void handleAck( boost::shared_ptr< pzq::socket_t > in,
			boost::shared_ptr< ackcache_t > ackCache );
        
        bool shouldSendReplica( std::string replicaSource ) const;
        
        int getDelayUntilNextBroadcast() const;
        
        int getDelayUntilNextNodeTimeout() const;
        
        void broadcastKeepAlive();
        void broadcastRemove( const std::string& id );
        
        void handleNodesMessage();
        void setTimeoutState();
	
	void checkReplica( const std::string& key,
			   const std::string& owner );
	
      private:
	cluster_t();
	cluster_t( const cluster_t& );
	cluster_t& operator=( const cluster_t& );
	
	void sendAndEraseNegativeAck( boost::shared_ptr< pzq::socket_t > in, boost::shared_ptr< ackcache_t > ackCache, std::string id );
	void sendAndErasePositiveAck( boost::shared_ptr< pzq::socket_t > in, boost::shared_ptr< ackcache_t > ackCache, std::string id );
	void sendAck( boost::shared_ptr< pzq::socket_t > in, pzq::message_t ack );
        void handleKeepAlive( pzq::message_t msg );
        void handleRemove( pzq::message_t msg );
	void handleCheck( pzq::message_t msg );
	
	void broadcastCheck( const std::string& id,
			     const std::string& owner );
	
	int                                   m_replicas;
	nodelist_t                            m_nodelist;
	int64_t                               m_timeoutNode;
        uint64_t                              m_nextBroadcast;
	boost::shared_ptr< pzq::socket_t >    m_out;
        boost::shared_ptr< pzq::socket_t >    m_pub;
        boost::shared_ptr< pzq::socket_t >    m_sub;
        boost::shared_ptr< pzq::datastore_t > m_store;
	std::string                           m_currentNode;
        bool                                  m_timeoutState;
     };
}

#endif // PZQ_CLUSTER_HPP
