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

#include "reaper.hpp"
#include "time.hpp"

void pzq::expiry_reaper_t::run ()
{
    while (is_running ())
    {
        m_time = microsecond_timestamp ();
        m_store.get ()->iterate_inflight (this);
        boost::this_thread::sleep (boost::posix_time::microseconds (m_frequency));
    }
}

const char *pzq::expiry_reaper_t::visit_full (const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz, size_t *sp)
{
    if (sizeof (uint64_t) != vsiz)
        return Visitor::NOP;

    uint64_t value;
    memcpy (&value, vbuf, sizeof (uint64_t));

	if (m_time - value > m_timeout)
	{
        std::cerr << "Record expired" << std::endl;
        m_store.get ()->message_expired ();
        return Visitor::REMOVE;
	}
    return Visitor::NOP;
}