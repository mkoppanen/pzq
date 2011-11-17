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

#include <boost/program_options.hpp>
#include <signal.h>

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

int main (int argc, char *argv []) 
{
    po::options_description desc ("Command-line options");
    po::variables_map vm;
    std::string filename;
    uid_t uid;
    int64_t inflight_size;
    uint64_t ack_timeout, reaper_frequency;
    std::string receiver_dsn, sender_dsn, monitor_dsn, peer_uuid;

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
        ("uid",
         po::value<uid_t> (&uid)->default_value (0),
        "User ID the process should run under")
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
    
    if(vm.count("uid") && uid > 0) {
        if(setuid(uid) == -1) {
            std::cerr << "Failed to become user" <<std::endl;
            exit(1);
        }
    }

    // Background
    if (vm.count ("background")) {
        if (daemonize () == -1) {
            exit (1);
        }
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
        in_socket.get ()->setsockopt (ZMQ_HWM, &in_hwm, sizeof (uint64_t));
        in_socket.get ()->bind (receiver_dsn.c_str ());

        boost::shared_ptr<pzq::socket_t> out_socket (new pzq::socket_t (context, ZMQ_DEALER));
        out_socket.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        out_socket.get ()->setsockopt (ZMQ_HWM, &out_hwm, sizeof (uint64_t));
        out_socket.get ()->bind (sender_dsn.c_str ());

        boost::shared_ptr<pzq::socket_t> monitor (new pzq::socket_t (context, ZMQ_ROUTER));
        monitor.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        monitor.get ()->setsockopt (ZMQ_HWM, &out_hwm, sizeof (uint64_t));
        monitor.get ()->bind (monitor_dsn.c_str ());

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
            manager.set_sockets (in_socket, out_socket, monitor);
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
