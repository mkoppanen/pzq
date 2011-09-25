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

    typedef char pzq_uuid_string_t [37];

    class i_datastore_t
    {
    public:
        // Called when the data store is opened
        virtual void open (const std::string &path, int64_t inflight_size) = 0;

        // Save a message and flags to datastore
        virtual bool save (const std::vector <pzq_message> &message_parts) = 0;

		// Delete message
		virtual void remove (const std::string &) = 0;

        // Close the store
        virtual void close () = 0;

		// sync to disk
		virtual void sync () = 0;

		// Whether message has been sent but no ACK yet
		virtual bool is_in_flight (const std::string &k) = 0;

		// Mark message sent (no ack yet)
		virtual void mark_in_flight (const std::string &k) = 0;

        // How many messages in total in store
        virtual int64_t messages () = 0;

        // Iterate using a visitor
        virtual void iterate (DB::Visitor *visitor) = 0;

        // virtual destructor
        virtual ~i_datastore_t () {}
    };

    class datastore_t : public i_datastore_t
    {
    protected:
        TreeDB db;
		CacheDB inflight_db;
        int m_divisor;
        int m_ack_timeout;
        bool m_hard_sync;
        uint64_t m_syncs, m_expiration;

    public:
        datastore_t () : m_divisor (0), m_ack_timeout (5), m_hard_sync (false), m_syncs (0), m_expiration (0)
        {}

        void open (const std::string &path, int64_t inflight_size);

        bool save (const std::vector <pzq_message> &message_parts);

		void remove (const std::string &key);

		void sync ();

        void close ();

        int64_t messages ()
        {
            return this->db.count ();
        }

        int64_t db_size ()
        {
            return this->db.size ();
        }

        int64_t messages_in_flight ()
        {
            return this->inflight_db.count ();
        }

        int64_t inflight_db_size ()
        {
            return this->inflight_db.size ();
        }

        uint64_t num_syncs ()
        {
            return m_syncs;
        }

        uint64_t num_expired ()
        {
            return m_expiration;
        }

		bool is_in_flight (const std::string &k);

		void mark_in_flight (const std::string &k);

        void set_sync_divisor (int divisor)
        {
            m_divisor = divisor;
        }

        void set_ack_timeout (int ack_timeout)
        {
            m_ack_timeout = ack_timeout;
        }

        void set_hard_sync (bool sync)
        {
            m_hard_sync = sync;
        }

        void iterate (DB::Visitor *visitor);

        ~datastore_t ();
    };

    class datastore_exception : public std::exception
    {
    private:
        std::string db_err;

    public:

        datastore_exception (const char *message)
        {
            db_err.append (message);
        }

        datastore_exception (const char *message, const BasicDB& db)
        {
            db_err.append (message);
            db_err.append (": ");
            db_err.append (db.error ().message ());
        }

        datastore_exception (const BasicDB& db)
        {
            db_err.append (db.error ().message ());
        }

        virtual const char* what() const throw()
        {
            return db_err.c_str ();
        }

        virtual ~datastore_exception() throw()
        {}
    };
}

#endif