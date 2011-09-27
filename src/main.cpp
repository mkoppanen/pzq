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
#include "device.hpp"
#include "manager.hpp"
#include "socket.hpp"
#include "visitor.hpp"
#include "terminator.hpp"
#include "sync.hpp"

#include <boost/program_options.hpp>
#include <signal.h>

namespace po = boost::program_options;

volatile sig_atomic_t keep_running = 1;

void time_to_go (int signum)
{
    keep_running = 0;
}

int main (int argc, char *argv []) 
{
    po::options_description desc ("Command-line options");
    po::variables_map vm;
    std::string filename;
    int sync_divisor;
    int64_t inflight_size;
    uint64_t ack_timeout, reaper_frequency, sync_frequency;
    bool hard_sync;
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
        ("sync-frequency",
          po::value<uint64_t> (&sync_frequency)->default_value (2500000),
         "How often to sync messages to disk (microseconds)")
    ;

    desc.add_options()
        ("sync-divisor",
          po::value<int> (&sync_divisor)->default_value (0),
         "The divisor for sync to the disk. 0 causes sync after every message")
    ;

    desc.add_options()
        ("hard-sync",
         "If enabled the data is flushed to disk on every sync")
    ;

    desc.add_options()
        ("inflight-size",
          po::value<int64_t> (&inflight_size)->default_value (31457280),
         "Maximum size in bytes for the in-flight messages database. Full database causes LRU collection")
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

    signal (SIGINT, time_to_go);
    signal (SIGHUP, time_to_go);
    signal (SIGTERM, time_to_go);

    // Init new zeromq context
    zmq::context_t *context = new zmq::context_t (1);

    {
        pzq::device_t receiver, sender;

        int linger = 1000;
        uint64_t hwm = 1;

        try {
            // Wire the receiver
            boost::shared_ptr<pzq::socket_t> receiver_in (new pzq::socket_t (*context, ZMQ_ROUTER));
            receiver_in.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
            receiver_in.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
            receiver_in.get ()->bind (receiver_dsn.c_str ());

            boost::shared_ptr<pzq::socket_t> receiver_out (new pzq::socket_t (*context, ZMQ_PAIR));
            receiver_out.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
            receiver_out.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
            receiver_out.get ()->bind ("inproc://receiver-inproc");

            // Wire the sender
            boost::shared_ptr<pzq::socket_t> sender_in (new pzq::socket_t (*context, ZMQ_PAIR));
            sender_in.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
            sender_in.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
            sender_in.get ()->bind ("inproc://sender-inproc");

            boost::shared_ptr<pzq::socket_t> sender_out (new pzq::socket_t (*context, ZMQ_DEALER));
            sender_out.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
            sender_out.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
            sender_out.get ()->bind (sender_dsn.c_str ());

            // Start the receiver device
            receiver.set_sockets (receiver_in, receiver_out);
            receiver.start ();

            // Start the sender device
            sender.set_sockets (sender_in, sender_out);
            sender.start ();
        } catch (std::exception &e) {
            std::cerr << "Error starting listening sockets: " << e.what () << std::endl;
            return 1;
        }

        try {
            // Start the store manager
            pzq::manager_t manager;

            boost::shared_ptr<pzq::socket_t> manager_in (new pzq::socket_t (*context, ZMQ_PAIR));
            manager_in.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
            manager_in.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
            manager_in.get ()->connect ("inproc://receiver-inproc");

            boost::shared_ptr<pzq::socket_t> manager_out (new pzq::socket_t (*context, ZMQ_PAIR));
            manager_out.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
            manager_out.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
            manager_out.get ()->connect ("inproc://sender-inproc");

            boost::shared_ptr<pzq::socket_t> monitor (new pzq::socket_t (*context, ZMQ_ROUTER));
            monitor.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
            monitor.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
            monitor.get ()->bind (monitor_dsn.c_str ());

            boost::shared_ptr<pzq::datastore_t> store (new pzq::datastore_t ());
            store.get ()->set_sync_divisor (sync_divisor);
            store.get ()->open (filename, inflight_size);

            // Reaper for expired messages
            pzq::expiry_visitor_t reaper (store);
            reaper.set_frequency (reaper_frequency);
            reaper.set_ack_timeout (ack_timeout);
            reaper.start ();

            // Syncing to disk
            pzq::sync_t sync (store);
            sync.set_frequency (sync_frequency);
            sync.start ();

            manager.set_datastore (store);
            manager.set_ack_timeout (ack_timeout);
            manager.set_sockets (manager_in, manager_out, monitor);

            manager.start ();

            while (keep_running)
            {
                boost::this_thread::sleep (boost::posix_time::seconds (1));
            }
            reaper.stop ();
            sync.stop ();
            sender.stop ();
            receiver.stop ();

            store.reset ();

        } catch (std::exception &e) {
            std::cerr << "Error starting store manager: " << e.what () << std::endl;
            return 1;
        }
    }
    exit (0);
}
