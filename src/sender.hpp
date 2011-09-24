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

#ifndef PZQ_SENDER_HPP
# define PZQ_SENDER_HPP

#include "pzq.hpp"
#include "store.hpp"
using namespace kyotocabinet;

namespace pzq
{
    class sender_t : public DB::Visitor
    {
    protected:
        boost::scoped_ptr<zmq::socket_t> m_socket;
		boost::shared_ptr<pzq::datastore_t> m_store;

    public:
        sender_t (zmq::context_t &ctx, std::string &dsn, bool use_pub);

		void set_datastore (boost::shared_ptr<pzq::datastore_t> store)
		{
			m_store = store;
		}

        bool can_write ();

    private:
        const char *visit_full (const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz, size_t *sp);

        const char *visit_empty (const char* kbuf, size_t ksiz, size_t* sp);

    };
}

#endif