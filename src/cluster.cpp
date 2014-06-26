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

#include "cluster.hpp"
#include "time.hpp"
#include "ackcache.hpp"

using std::vector;
using std::string;
using boost::shared_ptr;

namespace pzq
{
   cluster_t::cluster_t( int replicas, const vector< string >& nodeNames, uint64_t timeoutNode, 
			 boost::shared_ptr< pzq::socket_t > clusterSocket,
                         boost::shared_ptr< pzq::socket_t > broadcastSocket,
                         boost::shared_ptr< pzq::socket_t > subscribeSocket,
			 string currentNode,
                         boost::shared_ptr< pzq::datastore_t > store )
     {
	m_replicas = replicas;
	
	uint64_t curtime = microsecond_timestamp();
	
	for( vector< string >::const_iterator it = nodeNames.begin(); it != nodeNames.end(); ++it )
	  m_nodelist[ *it ] = curtime;
	
	m_timeoutNode = timeoutNode;
	m_out = clusterSocket;
        m_pub = broadcastSocket;
        m_sub = subscribeSocket;
	m_currentNode = currentNode;
        m_store= store;
	
        m_nextBroadcast = microsecond_timestamp();
        m_timeoutState = false;
	pzq::log( "Connect to cluster" );
     }
   
   cluster_t::~cluster_t()
     {
     }
   
   message_t cluster_t::createReplica( message_t orig ) const
     {
	message_t replica;
	
	message_iterator_t it = orig.begin();
	++it;
	
	replica.append( *it );
	++it;
	
	string srcReplica = "REPLICA:";
	srcReplica += m_currentNode;
	replica.append( srcReplica );
	
	while( it != orig.end() )
	  {
	     replica.append( *it );
	     ++it;
	  }
	
	return replica;
     }
   
   void cluster_t::setTimeoutState()
     {
        m_timeoutState = true;
     }
   
   int cluster_t::replicas() const
     {
	return m_replicas;
     }
   
   int cluster_t::countActiveNodes() const
     {
	uint64_t curtime = microsecond_timestamp();
	int count = 0;
        
	for( nodelist_t::const_iterator it = m_nodelist.begin(); it != m_nodelist.end(); ++it )
	  if( ( (int64_t)it->second - (int64_t)curtime ) < m_timeoutNode )
	    count++;
	
	return count;
     }
   
   bool cluster_t::shouldSendReplica( string replicaSource ) const
     {
        uint64_t curtime = microsecond_timestamp();
        
        nodelist_t::const_iterator it = m_nodelist.find( replicaSource );
        if( it != m_nodelist.end() &&
            ( (int64_t)it->second - (int64_t)curtime ) < m_timeoutNode )
          return false;
        
        return true;
     }
   
   /*
    * If a replica is here since m_timeoutNode we broadcast a checkmessage. 
    * The replica source will broadcast a remove message if it is no longer in its database
    */
   void cluster_t::checkReplica( const string& key,
				 const string& owner )
     {
	uint64_t curtime = microsecond_timestamp();
	
	string sts = key.substr( 0, key.find_first_of( '|' ) );
	std::istringstream ss( sts );
	
	uint64_t ts;
	ss >> ts;
	
	if( ( ( int64_t )curtime - ( int64_t )ts ) > m_timeoutNode )
	  broadcastCheck( key, owner );
     }
   
   boost::shared_ptr< pzq::socket_t > cluster_t::getOutSocket()
     {
	return m_out;
     }
   
   boost::shared_ptr< pzq::socket_t > cluster_t::getSubSocket()
     {
        return m_sub;
     }
   
   void cluster_t::handleAck( shared_ptr< pzq::socket_t > in,
			      shared_ptr< ackcache_t > ackCache )
     {
	pzq::message_t parts;
	
	if( m_out.get()->recv_many(parts) >= 2 )
	  {
	     message_iterator_t it = parts.begin();
	     string id = string( (char*)(*it)->data(), (*it)->size() );
	     ++it;
	     
	     string success = string( (char*)(*it)->data(), (*it)->size() );
	     if( success.size() == 0 || success[0] != '1' )
	       sendAndEraseNegativeAck( in, ackCache, id );
	     else
	       sendAndErasePositiveAck( in, ackCache, id );
	  }
     }
   
   void cluster_t::sendAndEraseNegativeAck( shared_ptr< pzq::socket_t > in,
					    shared_ptr< ackcache_t > ackCache,
					    std::string id )
     {
	pzq::message_t ack = ackCache->getAndRemoveById( id );
	if( ack.size() )
	  {
	     message_iterator_t it = ack.begin();
	     ++it;
	     ++it;
	     ((char*)(*it)->data())[0] = '0';
	     
	     sendAck( in, ack );
	  }
     }
   
   void cluster_t::sendAndErasePositiveAck( shared_ptr< pzq::socket_t > in,
					    shared_ptr< ackcache_t > ackCache,
					    std::string id )
     {
	pzq::message_t ack = ackCache->getAndRemoveById( id );
	if( ack.size() )
	  sendAck( in, ack );
        else
          broadcastRemove( id );
     }
   
   void cluster_t::sendAck( shared_ptr< pzq::socket_t > in,
			    pzq::message_t ack )
     {
	in->send_many( ack );
     }
   
   int cluster_t::getDelayUntilNextBroadcast() const
     {
        uint64_t curtime = microsecond_timestamp();
        return ( (int64_t)m_nextBroadcast - (int64_t)curtime ) / 1000;
     }
   
   int cluster_t::getDelayUntilNextNodeTimeout() const
     {
        uint64_t next = std::numeric_limits<uint64_t>::max();
        
        if( !m_timeoutState )
          {
             for( nodelist_t::const_iterator it = m_nodelist.begin(); it != m_nodelist.end(); ++it )
               {
                  if( it->second < next )
                    next = it->second;
               }
          }
        
        uint64_t curtime = microsecond_timestamp();
        return ( (int64_t)next - (int64_t)curtime ) / 1000;
     }
   
   void cluster_t::broadcastKeepAlive()
     {
        pzq::message_t kalv;
        kalv.append( "CLUSTER" );
        kalv.append( "KALV" );
        kalv.append( m_currentNode );
        
        m_pub->send_many( kalv );
        
        uint64_t curtime = microsecond_timestamp();
        m_nextBroadcast = curtime + m_timeoutNode / 10;
     }
   
   void cluster_t::broadcastRemove( const string& id )
     {
        pzq::message_t rm;
        rm.append( "CLUSTER" );
        rm.append( "REMOVE" );
        rm.append( id );
        
        m_pub->send_many( rm );
     }
   
   void cluster_t::broadcastCheck( const string& id,
				   const string& owner )
     {
	pzq::message_t chk;
	chk.append( "CLUSTER" );
	chk.append( "CHECK" );
	chk.append( id );
	chk.append( owner );
	
	m_pub->send_many( chk );
     }
   
   void cluster_t::handleNodesMessage()
     {
        pzq::message_t msg;
        if( m_sub->recv_many( msg ) > 1 )
          {
             msg.pop_front();
             pzq::message_part_t part = msg.front();
             string type = string( ( char* )part->data(), part->size() );
             
             if( type == "KALV" )
               handleKeepAlive( msg );
             else if( type == "REMOVE" )
               handleRemove( msg );
	     else if( type == "CHECK" )
	       handleCheck( msg );
          }
     }
   
   void cluster_t::handleKeepAlive( pzq::message_t msg )
     {
        msg.pop_front();
        
        pzq::message_part_t part = msg.front();
        string node = string( ( char* )part->data(), part->size() );
        
        uint64_t curtime = microsecond_timestamp();
        m_nodelist[ node ] = curtime;
        m_timeoutState = false;
     }
   
   void cluster_t::handleRemove( pzq::message_t msg )
     {
        msg.pop_front();
        
        pzq::message_part_t part = msg.front();
        string id = string( ( char* )part->data(), part->size() );
        
        try
          {
             m_store->removeReplica( id );
          }
        catch( pzq::datastore_exception e )
          {
             printf("could not find %s", id.c_str());
          }
     }
   
   void cluster_t::handleCheck( pzq::message_t msg )
     {
	msg.pop_front();
	
	pzq::message_part_t part = msg.front();
	string id = string( ( char* )part->data(), part->size() );
	
	msg.pop_front();
	
	part = msg.front();
	string owner = string( ( char* )part->data(), part->size() );
	
	if( owner == m_currentNode )
	  {
	     if( !m_store->check( id ) )
	       broadcastRemove( id );
	  }
     }
}
