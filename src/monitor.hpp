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

#ifndef PZQ_MONITOR_HPP
# define PZQ_MONITOR_HPP

#include "pzq.hpp"
#include "runnable.hpp"
#include "store.hpp"

namespace pzq {

    class monitor_t : public runnable_t
    {
    private:
        int m_frequency;
        boost::shared_ptr<pzq::datastore_t> m_store;
        boost::scoped_ptr<zmq::socket_t> m_socket;

    public:

        monitor_t (zmq::context_t &ctx, const std::string &dsn, int frequency);

		void set_datastore (boost::shared_ptr<pzq::datastore_t> store)
		{
			m_store = store;
		}

        void run ();
    };
}
#endif