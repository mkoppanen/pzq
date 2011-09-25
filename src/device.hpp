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

#ifndef PZQ_DEVICE_HPP
# define PZQ_DEVICE_HPP

#include "pzq.hpp"

namespace pzq {

    class device_t
    {
    private:
        boost::shared_ptr<zmq::socket_t> m_in;
        boost::shared_ptr<zmq::socket_t> m_out;
        boost::shared_ptr<boost::thread> m_thread;

    public:
        void set_sockets (boost::shared_ptr<zmq::socket_t> in, boost::shared_ptr<zmq::socket_t> out)
        {
            m_in = in;
            m_out = out;
        }

        void start ()
        {
            m_thread = boost::shared_ptr<boost::thread> (
                            new boost::thread (
                                boost::bind (&device_t::run, this)
                            )
                       );
        }

        void run ()
        {
            try {
                zmq::device (ZMQ_FORWARDER, *m_in.get (), *m_out.get ());
            } catch (zmq::error_t &e) {
                std::cerr << "Device terminated: " << e.what () << std::endl;
            }
        }

        ~device_t ()
        {
            std::cerr << "Terminating device" << std::endl;
        }
    };
}

#endif