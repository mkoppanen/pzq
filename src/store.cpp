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

#include "pzq.hpp"
#include "store.hpp"
#include "visitor.hpp"
#include "time.hpp"
#include <iostream>
#include <exception>
#include <boost/scoped_array.hpp>

void pzq::datastore_t::open (const std::string &path, int64_t inflight_size)
{
    std::string p = path;
    m_db.tune_defrag (8);

    if (m_db.open (p, TreeDB::OWRITER | TreeDB::OCREATE) == false)
        throw pzq::datastore_exception (m_db);

    pzq::log ("Loaded %lld messages from store", m_db.count ());

	m_inflight_db.cap_size (inflight_size);

	p.append (".inflight");
	if (m_inflight_db.open (p, CacheDB::OWRITER | CacheDB::OCREATE) == false)
		throw pzq::datastore_exception (m_db);

    // initialise cursor
	m_cursor.reset (m_db.cursor ());
    (*m_cursor).jump ();
}

bool pzq::datastore_t::save (pzq::message_t &parts, std::string extKey, std::string& storedKey)
{
    if (!parts.size ())
        throw std::runtime_error ("Trying to save empty message");

    pzq::uuid_string_t uuid_str;
	uuid_t uu;

	uuid_generate (uu);
    uuid_unparse (uu, uuid_str);

    std::stringstream kval;
    if( extKey == "" )
     {
        kval << pzq::microsecond_timestamp ();
        kval << "|" << uuid_str;
     }
   else
     {
        kval << extKey;
        printf("storing recplica: %s\n", kval.str().c_str());
     }

    bool success = true;

    m_db.begin_transaction (m_hard_sync);

    for (pzq::message_iterator_t it = parts.begin (); it != parts.end (); it++)
    {
        uint64_t size = (*it).get ()->size ();
        success = m_db.append (kval.str ().c_str (), kval.str ().size (),
                               (const char *) &size, sizeof (uint64_t));

        if (!success)
            break;

        success = m_db.append (kval.str ().c_str (), kval.str ().size (),
                               (const char *) (*it).get ()->data (), (*it).get ()->size ());
        if (!success)
            break;
    }

    if (!m_db.end_transaction (success))
        throw pzq::datastore_exception (m_db);

    if (!success)
        throw pzq::datastore_exception ("Failed to store the record");
   
    storedKey = kval.str();

    return true;
}

void pzq::datastore_t::sync ()
{
    if (!m_db.synchronize (m_hard_sync))
        throw pzq::datastore_exception (m_db);

	if (!m_inflight_db.synchronize (m_hard_sync))
        throw pzq::datastore_exception (m_inflight_db);

    m_syncs++;
}

void pzq::datastore_t::remove (const std::string &k)
{
    if (!m_inflight_db.remove (k.c_str (), k.size ()))
        throw pzq::datastore_exception (m_inflight_db);

	if (!m_db.remove (k))
	    throw pzq::datastore_exception (m_db);
}

void pzq::datastore_t::removeReplica( const std::string& k )
{
   if( !m_db.remove(k) )
     throw pzq::datastore_exception( m_db );
}

void pzq::datastore_t::remove_inflight (const std::string &k)
{
    if (!m_inflight_db.remove (k.c_str (), k.size ()))
        throw pzq::datastore_exception (m_inflight_db);
}

bool pzq::datastore_t::is_in_flight (const std::string &k)
{
	uint64_t value;
	if (m_inflight_db.get (k.c_str (), k.size (), (char *) &value, sizeof (uint64_t)) == -1)
	{
		return false;
	}

	if (pzq::microsecond_timestamp () - value > m_ack_timeout)
	{
		m_inflight_db.remove (k.c_str (), k.size ());
        message_expired ();
		return false;
	}
	return true;
}

void pzq::datastore_t::mark_in_flight (const std::string &k)
{
	uint64_t value = pzq::microsecond_timestamp ();
    m_inflight_db.add (k.c_str (), k.size (), (const char *) &value, sizeof (uint64_t));
}

void pzq::datastore_t::iterate (DB::Visitor *visitor)
{
    int expired = get_messages_expired ();

    while (true)
    {
        size_t key_size, value_size;
        const char *value;

        char *key = (*m_cursor).get (&key_size, &value, &value_size, true);

        if (!key)
        {
            (*m_cursor).jump ();
            throw std::runtime_error ("Failed to fetch record from store");
        }
        try {
            visitor->visit_full (key, key_size, value, value_size, NULL);
            delete [] key;
        } catch (std::exception &e) {
            delete [] key;
            throw e;
        }

        // if messages expire we move the cursor to beginning
        int current_expired = get_messages_expired ();
        if (expired != current_expired)
        {
            (*m_cursor).jump ();
        }
        expired = current_expired;
    }

#if 0
    if (!m_db.iterate (visitor, false))
        throw pzq::datastore_exception (m_db);
#endif
}

void pzq::datastore_t::iterate_inflight (DB::Visitor *visitor)
{
    if (!m_inflight_db.iterate (visitor, true))
        throw pzq::datastore_exception (m_db);

    m_inflight_db.synchronize ();
}

bool pzq::datastore_t::messages_pending ()
{
    if (m_db.count () == 0) {
        return false;
    }

    if (m_inflight_db.count () == m_db.count ())
        return false;

    return true;
}

pzq::datastore_t::~datastore_t ()
{
    pzq::log ("Closing down datastore, messages=[%lld] messages_inflight=[%lld]", m_db.count (), m_inflight_db.count ());
    m_db.close ();
    m_inflight_db.close ();
}

void pzq::datastore_t::resetIterator()
{
   m_cursor->jump();
}
