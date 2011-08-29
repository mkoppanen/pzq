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

#ifndef PZQ_RUNNABLE_HPP
# define PZQ_RUNNABLE_HPP

#include <boost/thread.hpp>

namespace pzq 
{
    class runnable_t 
    {
    protected:
        bool m_detached;
        boost::shared_ptr<boost::thread> m_thread;

    public:

        // Start the thread
        void start (bool detach = false)
        {
            m_thread = boost::shared_ptr<boost::thread> (new boost::thread (&runnable_t::run, this));

            if (detach)
                m_thread.get ()->detach ();

            m_detached = detach;
        }

        // Wait for the thread to finish
        void wait ()
        {
            if (m_detached)
                throw std::runtime_error ("The thread is detached");

            m_thread.get ()->join ();
        }
        virtual void run () = 0;
    };
}
#endif