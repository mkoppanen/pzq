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

namespace po = boost::program_options;

int main (int argc, char *argv []) 
{
    po::options_description desc("Command-line options");
    po::variables_map vm;
    std::string filename;
    int ack_timeout, sync_divisor;
    uint64_t inflight_size;

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
          po::value<int> (&sync_divisor)->default_value (1000),
         "The divisor for hard sync to the disk. 0 causes hard sync after every message")
    ;

    desc.add_options()
        ("inflight-size",
          po::value<uint64_t> (&inflight_size)->default_value (31457280),
         "Maximum size in bytes for the in-flight messages database. Full database causes LRU collection")
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

    zmq::context_t context (1);
    pzq::receiver_t receiver (context, filename, sync_divisor, inflight_size);

    receiver.start ();
    receiver.wait ();

    return 0;
}