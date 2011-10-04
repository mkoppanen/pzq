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

#ifndef PZQ_REAPER_HPP
# define PZQ_REAPER_HPP

#include "pzq.hpp"
#include "store.hpp"
#include "socket.hpp"
#include "time.hpp"
#include "thread.hpp"

using namespace kyotocabinet;

namespace pzq
{
    class expiry_reaper_t : public DB::Visitor, public thread_t
    {
    private:
        uint64_t m_time;
        uint64_t m_timeout;
        uint64_t m_frequency;
        boost::shared_ptr<pzq::datastore_t> m_store;

    public:
        expiry_reaper_t (boost::shared_ptr<pzq::datastore_t> store) : m_timeout (5000000), m_frequency (2500000), m_store (store)
        {}

        void set_frequency (uint64_t frequency)
        {
            m_frequency = frequency;
        }

        void set_ack_timeout (uint64_t timeout)
        {
            m_timeout = timeout;
        }

        void run ();

        const char *visit_full (const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz, size_t *sp);
    };
}

#endif