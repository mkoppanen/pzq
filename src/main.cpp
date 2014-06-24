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
#include "visitor.hpp"
#include "reaper.hpp"
#include "cluster.hpp"

#include <boost/program_options.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>
#include <signal.h>
#include <pwd.h>

namespace po = boost::program_options;

volatile sig_atomic_t keep_running = 1;

void time_to_go (int signum)
{
    keep_running = 0;
}

static
int daemonize ()
{
    signal(SIGINT, SIG_IGN);
	signal(SIGKILL, SIG_IGN);

	if (fork() == 0) {
	    // child
	    signal (SIGINT, time_to_go);
        signal (SIGHUP, time_to_go);
        signal (SIGTERM, time_to_go);

        if (chdir ("/") == -1) {
            std::cerr << "Failed to chdir: " << strerror (errno) << std::endl;
            return -1;
        }
        fclose(stdin);
		fclose(stdout);
		fclose(stderr);
    } else {
        // parent
        signal(SIGINT, SIG_DFL);
		signal(SIGKILL, SIG_DFL);
		exit(0);
    }
    return 0;
}

bool drop_privileges(uid_t uid, gid_t gid)
{
    if (setgid(gid) != 0) {
        return false;
    }

    if (setuid(uid) != 0) {
        return false;
    }
    
    return true;
}

static std::string buildBroadcastDsn( const std::string& nodeDsn, const std::string& currentNodeDsn )
{
   std::vector< std::string > nodeDsnParts;
   split( nodeDsnParts, nodeDsn, boost::is_any_of(":"), boost::algorithm::token_compress_on );
   
   std::vector< std::string > currentNodeDsnParts;
   split( currentNodeDsnParts, currentNodeDsn, boost::is_any_of(":"), boost::algorithm::token_compress_on );
   
   if( nodeDsnParts.size() == 3 && currentNodeDsnParts.size() == 3 )
     {
        std::vector< std::string > broadcastDsnParts;
        broadcastDsnParts.push_back( nodeDsnParts[ 0 ] );
        broadcastDsnParts.push_back( nodeDsnParts[ 1 ] );
        broadcastDsnParts.push_back( currentNodeDsnParts[ 2 ] );
        printf("%s\n", boost::algorithm::join( broadcastDsnParts, ":" ).c_str());
        return boost::algorithm::join( broadcastDsnParts, ":" );
     }
   else
     {
        std::cerr << "failed to parse all dsn" << std::endl;
        exit(1);
     }
}

static std::string buildSubscribeDsn( const std::string& currentNodeDsn )
{
   std::vector< std::string > currentNodeDsnParts;
   split( currentNodeDsnParts, currentNodeDsn, boost::is_any_of(":"), boost::algorithm::token_compress_on );
   
   if( currentNodeDsnParts.size() == 3 )
     {
        currentNodeDsnParts[ 1 ] = "//*";
        printf("sub:%s\n", boost::algorithm::join( currentNodeDsnParts, ":" ).c_str());
        return boost::algorithm::join( currentNodeDsnParts, ":" );
     }
   else
     {
        std::cerr << "failed to parse all dsn" << std::endl;
        exit(1);
     }
}

int main (int argc, char *argv []) 
{
    po::options_description desc ("Command-line options");
    po::variables_map vm;
    std::string filename;
    std::string user;
    int64_t inflight_size;
    uint64_t ack_timeout, reaper_frequency, timeoutNode, timeoutReplication;
    std::string receiver_dsn, sender_dsn, monitor_dsn, peer_uuid, nodes, currentNode_dsn;
    int32_t replicas;

    desc.add_options ()
        ("help", "produce help message");

    desc.add_options()
        ("database",
          po::value<std::string> (&filename)->default_value ("/tmp/sink.kch"),
         "Database sink file location")
    ;

    desc.add_options()
        ("ack-timeout",
          po::value<uint64_t> (&ack_timeout)->default_value (5000000),
         "How long to wait for ACK before resending message (microseconds)")
    ;

    desc.add_options()
        ("reaper-frequency",
          po::value<uint64_t> (&reaper_frequency)->default_value (2500000),
         "How often to clean up expired messages (microseconds)")
    ;

    desc.add_options()
        ("hard-sync",
         "If enabled the data is flushed to disk on every sync")
    ;

    desc.add_options()
        ("background",
         "Run in daemon mode")
    ;

    desc.add_options()
        ("inflight-size",
          po::value<int64_t> (&inflight_size)->default_value (31457280),
         "Maximum size in bytes for the in-flight messages database. Full database causes LRU collection")
    ;
    
    desc.add_options()
        ("user",
        po::value<std::string> (&user)->default_value (""),
        "User the process should run under")
    ;

    desc.add_options()
        ("receive-dsn",
          po::value<std::string> (&receiver_dsn)->default_value ("tcp://*:11131"),
         "The DSN for the receive socket")
    ;

    desc.add_options()
        ("send-dsn",
          po::value<std::string> (&sender_dsn)->default_value ("tcp://*:11132"),
         "The DSN for the backend client communication socket")
    ;

    desc.add_options()
        ("monitor-dsn",
          po::value<std::string> (&monitor_dsn)->default_value ("ipc:///tmp/pzq-monitor"),
         "The DSN for the monitoring socket")
    ;
   
    desc.add_options()
        ("replicas",
	 po::value<int32_t>(&replicas)->default_value(0),
	 "Number of replicas that should created before acknowledging message to producer")
    ;
   
    desc.add_options()
        ("nodes",
	 po::value<std::string >(&nodes)->default_value(""),
	 "List of DSN for other cluster nodes separated by a ','")
    ;
   
    desc.add_options()
        ("timeout-nodes",
	 po::value<uint64_t>(&timeoutNode)->default_value(10000000),
	 "How long to wait before considering a node down (microseconds)")
    ;
   
    desc.add_options()
        ("broadcast-dsn",
	 po::value<std::string>(&currentNode_dsn)->default_value(""),
	 "DSN used by other cluster nodes to broadcast msessages. Broardcast port should be the same for all nodes")
    ;
   
    desc.add_options()
        ("timeout_replication",
         po::value<uint64_t>(&timeoutReplication)->default_value(100000),
         "How long to wait for replication before acknowledging producer with a replication error message")
    ;

    try {
        po::store (po::parse_command_line (argc, argv, desc), vm);
        po::notify (vm);
    } catch (po::error &e) {
        std::cerr << "Error parsing command-line options: " << e.what () << std::endl;
        std::cerr << desc << std::endl;
        return 1;
    }
    if (vm.count ("help")) {
        std::cerr << desc << std::endl;
        return 1;
    }
    
    if (vm.count ("user") && user.length() != 0) {
        struct passwd *res_user;
        
        res_user = getpwnam( user.c_str() );
        if ( !res_user ) {
            std::cerr << "Could not find user " << user << std::endl;
            exit(1);
        }
        
        if ( !drop_privileges ( res_user->pw_uid, res_user->pw_gid ) ) {
            std::cerr << "Failed to become user" << user << std::endl;
            exit(1);
        }
    }
   
    // Parse cluster nodes names
    std::vector< std::string > nodeNames;
    if( nodes != "" )
     split( nodeNames, nodes, boost::is_any_of(","), boost::algorithm::token_compress_on );


    // Background
    if (vm.count ("background")) {
        if (daemonize () == -1) {
            exit (1);
        }
    } else {
        signal (SIGINT, time_to_go);
        signal (SIGHUP, time_to_go);
        signal (SIGTERM, time_to_go);
    }

    // Init new zeromq context
    zmq::context_t context (1);

    {
        int linger = 1000;
        uint64_t in_hwm = 10, out_hwm = 1;

        boost::shared_ptr<pzq::datastore_t> store (new pzq::datastore_t ());
        store.get ()->open (filename, inflight_size);
        store.get ()->set_ack_timeout (ack_timeout);

        boost::shared_ptr<pzq::socket_t> in_socket (new pzq::socket_t (context, ZMQ_ROUTER));
        in_socket.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        in_socket.get ()->setsockopt (ZMQ_SNDHWM, &in_hwm, sizeof (uint32_t));
        in_socket.get ()->setsockopt (ZMQ_RCVHWM, &in_hwm, sizeof (uint32_t));
        in_socket.get ()->bind (receiver_dsn.c_str ());

        boost::shared_ptr<pzq::socket_t> out_socket (new pzq::socket_t (context, ZMQ_DEALER));
        out_socket.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        out_socket.get ()->setsockopt (ZMQ_SNDHWM, &out_hwm, sizeof (uint32_t));
        out_socket.get ()->setsockopt (ZMQ_RCVHWM, &out_hwm, sizeof (uint32_t));
        out_socket.get ()->bind (sender_dsn.c_str ());

        boost::shared_ptr<pzq::socket_t> monitor (new pzq::socket_t (context, ZMQ_ROUTER));
        monitor.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        monitor.get ()->setsockopt (ZMQ_SNDHWM, &out_hwm, sizeof (uint32_t));
        monitor.get ()->setsockopt (ZMQ_RCVHWM, &out_hwm, sizeof (uint32_t));
        monitor.get ()->bind (monitor_dsn.c_str ());

        boost::shared_ptr<pzq::socket_t> clusterSocket( new pzq::socket_t( context, ZMQ_DEALER ) );
        clusterSocket.get()->setsockopt( ZMQ_LINGER, &linger, sizeof( int ) );
        clusterSocket.get()->setsockopt( ZMQ_SNDHWM, &out_hwm, sizeof( uint32_t ) );
        clusterSocket.get()->setsockopt( ZMQ_RCVHWM, &out_hwm, sizeof( uint32_t ) );
        for( std::vector< std::string >::iterator it = nodeNames.begin(); it != nodeNames.end(); ++it )
	 clusterSocket.get()->connect( it->c_str() );

        boost::shared_ptr<pzq::socket_t> broadcastSocket( new pzq::socket_t( context, ZMQ_PUB ) );
        broadcastSocket.get()->setsockopt( ZMQ_LINGER, &linger, sizeof( int ) );
        broadcastSocket.get()->setsockopt( ZMQ_SNDHWM, &out_hwm, sizeof( uint32_t ) );
        broadcastSocket.get()->setsockopt( ZMQ_RCVHWM, &out_hwm, sizeof( uint32_t ) );
        for( std::vector< std::string >::iterator it = nodeNames.begin(); it != nodeNames.end(); ++it )
         broadcastSocket.get()->connect( buildBroadcastDsn( *it, currentNode_dsn ).c_str() );

        boost::shared_ptr<pzq::socket_t> subscribeSocket( new pzq::socket_t( context, ZMQ_SUB ) );
        subscribeSocket.get()->setsockopt( ZMQ_LINGER, &linger, sizeof( int ) );
        subscribeSocket.get()->setsockopt( ZMQ_SNDHWM, &in_hwm, sizeof( uint32_t ) );
        subscribeSocket.get()->setsockopt( ZMQ_RCVHWM, &in_hwm, sizeof( uint32_t ) );
        subscribeSocket.get()->setsockopt( ZMQ_SUBSCRIBE, "CLUSTER", 7 );
        subscribeSocket.get()->bind( buildSubscribeDsn( currentNode_dsn ).c_str() );
         
        boost::shared_ptr< pzq::cluster_t > cluster( new pzq::cluster_t( replicas, nodeNames, timeoutNode, 
                                                                         clusterSocket, broadcastSocket, subscribeSocket, currentNode_dsn, store ) );
       
        boost::shared_ptr< pzq::ackcache_t > ackCache( new pzq::ackcache_t( timeoutReplication ) );

        try {
            // Start the store manager
            pzq::manager_t manager;

            // Reaper for expired messages
            pzq::expiry_reaper_t reaper (store);
            reaper.set_frequency (reaper_frequency);
            reaper.set_ack_timeout (ack_timeout);
            reaper.start ();

            manager.set_datastore (store);
            manager.set_ack_timeout (ack_timeout);
            manager.set_sockets (in_socket, out_socket, monitor, cluster);
	    manager.set_cluster( cluster );
	    manager.set_ack_cache( ackCache );
            manager.start ();

            while (keep_running)
            {
                boost::this_thread::sleep (
                    boost::posix_time::seconds (1)
                );
            }
            manager.stop ();
            reaper.stop ();

        } catch (std::exception &e) {
            pzq::log ("Error running store manager: %s", e.what ());
            return 1;
        }
    }
    pzq::log ("Terminating");
    return 0;
}
