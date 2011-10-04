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

    m_db.tune_comparator (DECIMALCOMP);
    m_db.tune_defrag (8);

    if (m_db.open (p, TreeDB::OWRITER | TreeDB::OCREATE) == false)
        throw pzq::datastore_exception (m_db);

    pzq::log ("Loaded %lld messages from store", m_db.count ());
	p.append (".inflight");

	m_inflight_db.cap_size (inflight_size);
	if (m_inflight_db.open (p, CacheDB::OWRITER | CacheDB::OCREATE) == false)
		throw pzq::datastore_exception (m_db);
}

bool pzq::datastore_t::save (pzq::message_t &parts)
{
    if (!parts.size ())
        throw std::runtime_error ("Trying to save empty message");

    pzq::uuid_string_t uuid_str;
	uuid_t uu;

	uuid_generate (uu);
    uuid_unparse (uu, uuid_str);

    std::stringstream kval;
    kval << pzq::microsecond_timestamp ();
    kval << "|" << uuid_str;

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

    sync ();
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
		return false;

	if (pzq::microsecond_timestamp () - value > m_ack_timeout)
	{
		m_inflight_db.remove (k.c_str (), k.size ());
		m_inflight_db.synchronize ();
        message_expired ();
		return false;
	}
	return true;
}

void pzq::datastore_t::mark_in_flight (const std::string &k)
{
	uint64_t value = pzq::microsecond_timestamp ();
    m_inflight_db.add (k.c_str (), k.size (), (const char *) &value, sizeof (uint64_t));
    m_inflight_db.synchronize ();
}

void pzq::datastore_t::iterate (DB::Visitor *visitor)
{
#if 0
    if (!m_cursor.get ())
    {
        m_cursor.reset (m_db.cursor ());
        m_cursor.get ()->jump ();
    }

    while (true)
    {
        size_t key_size, value_size;

        boost::scoped_array<char> key (m_cursor.get ()->get_key (&key_size));
        boost::scoped_array<char> value (m_cursor.get ()->get_value (&value_size));

        if (key.get () && value.get ())
            visitor->visit_full (key.get (), key_size, value.get (), value_size, NULL);

        // End of records
        if (!m_cursor.get ()->step ())
        {
            m_cursor.get ()->jump ();
            throw new pzq::datastore_exception ("End of records");
        }
    }
#endif
    if (!m_db.iterate (visitor, false))
        throw pzq::datastore_exception (m_db);
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