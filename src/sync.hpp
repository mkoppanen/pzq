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

#ifndef PZQ_SYNC_HPP
# define PZQ_SYNC_HPP

namespace pzq {

    class sync_t : public thread_t {

    private:
        uint64_t m_frequency;
        boost::shared_ptr<pzq::datastore_t> m_store;

    public:
        sync_t (boost::shared_ptr<pzq::datastore_t> store) : m_store (store), m_frequency (500000)
        {
        }

        void run ()
        {
            while (is_running ())
            {
                m_store.get ()->sync ();
                boost::this_thread::sleep (boost::posix_time::microseconds (m_frequency));
            }
        }

        void set_frequency (uint64_t frequency)
        {
            m_frequency = frequency;
        }
    };
}


#endif
