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

    srand (time (NULL));

    this->db.tune_comparator (DECIMALCOMP);
    this->db.tune_defrag (8);

    if (this->db.open (p, TreeDB::OWRITER | TreeDB::OCREATE) == false)
        throw pzq::datastore_exception (this->db);

    std::cerr << "Loaded " << this->db.count () << " messages from store" << std::endl;

	p.append (".inflight");

	this->inflight_db.cap_size (inflight_size);
	if (this->inflight_db.open (p, CacheDB::OWRITER | CacheDB::OCREATE) == false)
		throw pzq::datastore_exception (this->db);
}

bool pzq::datastore_t::save (pzq::message_t &parts)
{
    pzq_uuid_string_t uuid_str;
	uuid_t uu;

	uuid_generate (uu);
    uuid_unparse (uu, uuid_str);

    std::stringstream kval;
    kval << pzq::microsecond_timestamp ();
    kval << "|" << uuid_str;

    this->db.begin_transaction ();

    for (pzq::message_iterator_t it = parts.begin (); it != parts.end (); it++)
    {
        size_t size = (*it).get ()->size ();
        this->db.append (kval.str ().c_str (), kval.str ().size (), (const char *) &size, sizeof (size_t));
        this->db.append (kval.str ().c_str (), kval.str ().size (),
                         (const char *) (*it).get ()->data (), (*it).get ()->size ());
    }
    this->db.end_transaction ();

    return true;
}

void pzq::datastore_t::sync ()
{
    if (!this->db.synchronize (m_hard_sync))
        throw pzq::datastore_exception (this->db);

	if (!this->inflight_db.synchronize (m_hard_sync))
        throw pzq::datastore_exception (this->inflight_db);

    m_syncs++;
}

void pzq::datastore_t::remove (const std::string &key)
{
    if (!this->inflight_db.remove (key.c_str (), key.size ()))
        throw pzq::datastore_exception (this->inflight_db);

	if (!this->db.remove (key))
	    throw pzq::datastore_exception (this->db);
}

void pzq::datastore_t::close ()
{
    this->db.close ();
}

bool pzq::datastore_t::is_in_flight (const std::string &k)
{
	uint64_t value;
	if (this->inflight_db.get (k.c_str (), k.size (), (char *) &value, sizeof (uint64_t)) == -1)
		return false;

	if (pzq::microsecond_timestamp () - value > m_ack_timeout)
	{
		this->inflight_db.remove (k.c_str (), k.size ());
        message_expired ();
		return false;
	}
	return true;
}

void pzq::datastore_t::mark_in_flight (const std::string &k)
{
	uint64_t value = pzq::microsecond_timestamp ();
    this->inflight_db.add (k.c_str (), k.size (), (const char *) &value, sizeof (uint64_t));
}

void pzq::datastore_t::iterate (DB::Visitor *visitor)
{
    if (!this->db.iterate (visitor, false))
        throw pzq::datastore_exception (this->db);
}

void pzq::datastore_t::iterate_inflight (DB::Visitor *visitor)
{
    if (!this->inflight_db.iterate (visitor, true))
        throw pzq::datastore_exception (this->db);
}

bool pzq::datastore_t::messages_pending ()
{
    if (this->db.count () == 0) {
        return false;
    }

    this->inflight_db.occupy ();
    if (this->inflight_db.count () != this->db.count ())
        return true;
}

pzq::datastore_t::~datastore_t ()
{
    db.close ();
    inflight_db.close ();
    std::cerr << "Closing down datastore" << std::endl;
}