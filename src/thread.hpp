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

#ifndef PZQ_THREAD_HPP
# define PZQ_THREAD_HPP

#include "pzq.hpp"
#include <pthread.h>

namespace pzq {

    class thread_t
    {
    private:
        bool m_running;
        boost::mutex m_mutex;
        boost::thread *m_thread;

    public:
        bool is_running ()
        {
            m_mutex.lock ();
            bool running = m_running;
            m_mutex.unlock ();
            return running;
        }        

        void stop ()
        {
            m_mutex.lock ();
            m_running = false;
            m_mutex.unlock ();
        }

        void start ()
        {
            m_running = true;
            m_thread = new boost::thread (
                            boost::bind (&thread_t::set_signals_and_run, this)
                            );
        }

        void set_signals_and_run ()
        {
            sigset_t new_mask;
            sigfillset (&new_mask);
            sigdelset (&new_mask, SIGINT);

            sigset_t old_mask;
            pthread_sigmask (SIG_BLOCK, &new_mask, &old_mask);

            run ();
        }

        virtual void run () = 0;

        virtual ~thread_t ()
        {
            stop ();
            m_thread->interrupt ();

            // Try to join the thread
            if (!m_thread->timed_join (boost::posix_time::seconds (2))) {
                pthread_t handle = m_thread->native_handle ();

                // Try to send SIGINT to the thread
                pthread_kill (handle, SIGINT);

                if (!m_thread->timed_join (boost::posix_time::seconds (2))) {
                    pthread_cancel (handle);
                }
            }
            delete m_thread;
        }
    };
}

#endif