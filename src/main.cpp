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
#include "receiver.hpp"
#include "sender.hpp"
#include <boost/program_options.hpp>
#include <signal.h>

namespace po = boost::program_options;

sig_atomic_t keep_running = 1;

void
time_to_go (int signum)
{
    keep_running = 0;
}

int main (int argc, char *argv []) 
{
    po::options_description desc("Command-line options");
    po::variables_map vm;
    std::string filename;
    int ack_timeout, sync_divisor;
    uint64_t inflight_size;
    bool hard_sync;
    std::string receive_dsn, ack_dsn, publish_dsn;

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
          po::value<uint64_t> (&inflight_size)->default_value (31457280),
         "Maximum size in bytes for the in-flight messages database. Full database causes LRU collection")
    ;

    desc.add_options()
        ("receive-dsn",
          po::value<std::string> (&receive_dsn)->default_value ("tcp://*:11131"),
         "The DSN for the receive socket")
    ;

    desc.add_options()
        ("ack-dsn",
          po::value<std::string> (&ack_dsn)->default_value ("tcp://*:11132"),
         "The DSN for the ACK socket")
    ;

    desc.add_options()
        ("publish-dsn",
          po::value<std::string> (&publish_dsn)->default_value ("tcp://*:11133"),
         "The DSN for the backend client communication socket")
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

    // Wire the receiver
    pzq::receiver_t receiver (context, filename, sync_divisor, inflight_size);
    receiver.set_sender (sender);
    receiver.set_datastore (store);

    // Install signal handlers
    if (signal (SIGINT, time_to_go) == SIG_IGN)
      signal (SIGINT, SIG_IGN);
    if (signal (SIGHUP, time_to_go) == SIG_IGN)
      signal (SIGHUP, SIG_IGN);
    if (signal (SIGTERM, time_to_go) == SIG_IGN)
      signal (SIGTERM, SIG_IGN);

    receiver.start ();
    receiver.wait ();

    return 0;
}