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
    int ack_timeout, sync_divisor;
    int64_t inflight_size;
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
          po::value<int> (&ack_timeout)->default_value (5),
         "How long to wait for ACK before resending message (seconds)")
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
        ("publish-dsn",
          po::value<std::string> (&sender_dsn)->default_value ("tcp://*:11132"),
         "The DSN for the backend client communication socket")
    ;

    desc.add_options()
        ("monitor-dsn",
          po::value<std::string> (&monitor_dsn)->default_value ("ipc:///tmp/pzq-monitor"),
         "The DSN for the monitoring socket")
    ;

    desc.add_options()
        ("uuid",
          po::value<std::string> (&peer_uuid),
         "UUID for this instance of PZQ. If none is set one is generated automatically")
    ;

    desc.add_options()
        ("use-pubsub",
         "Changes the backend client communication socket to use publish subscribe pattern")
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

    // Init new zeromq context
    zmq::context_t context (1);

    int linger = 1000;
    uint64_t hwm = 1;

    pzq::device_t receiver, sender;

    try {
        // Wire the receiver
        boost::shared_ptr<zmq::socket_t> receiver_in (new zmq::socket_t (context, ZMQ_ROUTER));
        receiver_in.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        receiver_in.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
        receiver_in.get ()->bind (receiver_dsn.c_str ());

        boost::shared_ptr<zmq::socket_t> receiver_out (new zmq::socket_t (context, ZMQ_PAIR));
        receiver_out.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        receiver_out.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
        receiver_out.get ()->bind ("inproc://receiver-inproc");

        // Wire the sender
        boost::shared_ptr<zmq::socket_t> sender_in (new zmq::socket_t (context, ZMQ_PAIR));
        sender_in.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        sender_in.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
        sender_in.get ()->bind ("inproc://sender-inproc");

        boost::shared_ptr<zmq::socket_t> sender_out (new zmq::socket_t (context, ZMQ_DEALER));
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

    // Start the store manager
    pzq::manager_t manager;

    try {
        boost::shared_ptr<zmq::socket_t> manager_in (new zmq::socket_t (context, ZMQ_PAIR));
        manager_in.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        manager_in.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
        manager_in.get ()->connect ("inproc://receiver-inproc");

        boost::shared_ptr<zmq::socket_t> manager_out (new zmq::socket_t (context, ZMQ_PAIR));
        manager_out.get ()->setsockopt (ZMQ_LINGER, &linger, sizeof (int));
        manager_out.get ()->setsockopt (ZMQ_HWM, &hwm, sizeof (uint64_t));
        manager_out.get ()->connect ("inproc://sender-inproc");

        boost::shared_ptr<pzq::datastore_t> store (new pzq::datastore_t ());
        store.get ()->set_sync_divisor (sync_divisor);
        store.get ()->set_ack_timeout (ack_timeout);
        store.get ()->open (filename, inflight_size);
        manager.set_datastore (store);

        manager.set_sockets (manager_in, manager_out);
        manager.start ();
    } catch (std::exception &e) {
        std::cerr << "Error starting store manager: " << e.what () << std::endl;
        return 1;
    }

    return 0;
#if 0
    // Init datastore
    boost::shared_ptr<pzq::datastore_t> store (new pzq::datastore_t ());
    store.get ()->set_sync_divisor (sync_divisor);
    store.get ()->open (filename, inflight_size);
    store.get ()->set_ack_timeout (ack_timeout);

    if (vm.count ("hard-sync")) {
        store.get ()->set_hard_sync (true);
    }

    // Init sender
    bool use_pubsub = vm.count ("use-pubsub") ? true : false;
    boost::shared_ptr<pzq::sender_t> sender (new pzq::sender_t (context, publish_dsn, use_pubsub));
    sender.get ()->set_datastore (store);

    if (peer_uuid.size () > 0)
    {
        try {
            sender.get ()->set_peer_uuid (peer_uuid);
        } catch (std::runtime_error &e) {
            std::cerr << e.what () << std::endl;
            return 1;
        }
    }

    // Wire the receiver
    pzq::receiver_t receiver (context, filename, sync_divisor, inflight_size);
    receiver.set_sender (sender);
    receiver.set_datastore (store);

    // Monitoring
    pzq::monitor_t monitor (context, monitor_dsn, 10);
    monitor.set_datastore (store);
    monitor.start ();

    // Install signal handlers
    signal (SIGINT, time_to_go);
    signal (SIGHUP, time_to_go);
    signal (SIGTERM, time_to_go);

    receiver.start ();
    receiver.wait ();
    monitor.wait ();
#endif
    return 0;
}