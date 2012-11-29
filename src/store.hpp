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

#ifndef PZQ_STORE_HPP
# define PZQ_STORE_HPP

#include "pzq.hpp"

using namespace kyotocabinet;

namespace pzq {

    typedef char uuid_string_t [37];

    class datastore_t
    {
    protected:
        TreeDB m_db;
		CacheDB m_inflight_db;
        boost::scoped_ptr<TreeDB::Cursor> m_cursor;
        uint64_t m_ack_timeout;
        bool m_hard_sync;
        uint64_t m_syncs;
        int m_expired;
        boost::mutex m_mutex;

    public:
        datastore_t () : m_ack_timeout (5000000ULL), m_hard_sync (false), m_syncs (0),
                         m_expired (0)
        {}

        void open (const std::string &path, int64_t inflight_size);

        bool save (pzq::message_t &message_parts);

		void remove (const std::string &key);

        void remove_inflight (const std::string &k);

		void sync ();

        int64_t messages ()
        {
            return m_db.count ();
        }

        int64_t db_size ()
        {
            return m_db.size ();
        }

        int64_t messages_inflight ()
        {
            return m_inflight_db.count ();
        }

        int64_t inflight_db_size ()
        {
            return m_inflight_db.size ();
        }

        uint64_t num_syncs ()
        {
            return m_syncs;
        }

        bool messages_pending ();

		bool is_in_flight (const std::string &k);

		void mark_in_flight (const std::string &k);

        void set_ack_timeout (uint64_t ack_timeout)
        {
            m_ack_timeout = ack_timeout;
        }

        uint64_t get_ack_timeout () const
        {
            return m_ack_timeout;
        }

        void set_hard_sync (bool sync)
        {
            m_hard_sync = sync;
        }

        int get_messages_expired ()
        {
            m_mutex.lock ();
            int expired = m_expired;
            m_mutex.unlock ();
            return expired;
        }

        void message_expired ()
        {
            m_mutex.lock ();
            m_expired++;
            m_mutex.unlock ();
        }

        void iterate (DB::Visitor *visitor);

        void iterate_inflight (DB::Visitor *visitor);

        ~datastore_t ();
    };

    class datastore_exception : public std::exception
    {
    private:
        std::string m_db_err;

    public:

        datastore_exception (const char *message)
        {
            m_db_err.append (message);
        }

        datastore_exception (const char *message, const BasicDB& db)
        {
            m_db_err.append (message);
            m_db_err.append (": ");
            m_db_err.append (db.error ().message ());
        }

        datastore_exception (const BasicDB& db)
        {
            m_db_err.append (db.error ().message ());
        }

        virtual const char* what() const throw()
        {
            return m_db_err.c_str ();
        }

        virtual ~datastore_exception() throw()
        {}
    };
}

#endif