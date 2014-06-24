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

#ifndef PZQ_VISITOR_HPP
# define PZQ_VISITOR_HPP

#include "pzq.hpp"
#include "store.hpp"
#include "socket.hpp"
#include "time.hpp"
#include "thread.hpp"
#include "cluster.hpp"

using namespace kyotocabinet;

namespace pzq
{
    class visitor_t : public DB::Visitor
    {
    private:
        boost::shared_ptr<pzq::socket_t> m_socket;
		boost::shared_ptr<pzq::datastore_t> m_store;
        uuid_t m_uuid;
        boost::shared_ptr< pzq::cluster_t > m_cluster;

    public:
        visitor_t ()
        {
            uuid_generate (m_uuid);
        }

        void set_socket (boost::shared_ptr<pzq::socket_t> socket,
                         boost::shared_ptr<pzq::cluster_t> cluster)
        {
            m_socket = socket;
            m_cluster = cluster;
        }

        void set_datastore (boost::shared_ptr<pzq::datastore_t> store)
        {
            m_store = store;
        }

        bool can_write ();

    private:
        const char *visit_full (const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz, size_t *sp);

    };
}

#endif