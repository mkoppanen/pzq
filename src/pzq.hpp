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

#ifndef PZQ_HPP
# define PZQ_HPP

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <zmq.hpp>
#include <kchashdb.h>
#include <kccachedb.h>
#include <uuid/uuid.h>
#include <stdarg.h>

namespace pzq 
{
    typedef boost::shared_ptr <zmq::message_t> message_part_t;

    typedef std::list <pzq::message_part_t>::iterator message_iterator_t;
    typedef std::list <pzq::message_part_t>::const_iterator message_const_iterator_t;

    class message_t 
    {
    private:   
        boost::shared_ptr <std::list <pzq::message_part_t> > m_message;

    public:
        message_iterator_t begin () { return (*m_message).begin (); }
        message_const_iterator_t end () { return (*m_message).end (); }

        message_t () : m_message (new std::list <pzq::message_part_t>)
        {}

        void append (message_part_t part)
        {
            (*m_message).push_back (part);
        }

        void append (const void *data, size_t size)
        {
            message_part_t part (new zmq::message_t (size));
            memcpy (part.get ()->data (), data, size);
            (*m_message).push_back (part);
        }

        void append (const std::string &str)
        {
            message_part_t part (new zmq::message_t (str.size ()));
            memcpy (part.get ()->data (), str.data (), str.size ());
            (*m_message).push_back (part);
        }

        void append (zmq::message_t &msg)
        {
            message_part_t part (new zmq::message_t);
            part.get ()->copy (&msg);
            (*m_message).push_back (part);
        }

        void append ()
        {
            message_part_t part (new zmq::message_t);
            (*m_message).push_back (part);
        }

        message_part_t front ()
        {
            return (*m_message).front ();
        }

        void front (std::string &str)
        {
            str.assign (static_cast <char *> ((*(*m_message).front ()).data ()),
                        (*(*m_message).front ()).size ());
        }

        message_part_t back ()
        {
            return (*m_message).back ();
        }

        void back (std::string &str)
        {
            str.assign (static_cast <char *> ((*(*m_message).back ()).data ()),
                        (*(*m_message).back ()).size ());
        }

        message_part_t pop_front ()
        {
            message_part_t part = (*m_message).front ();
            (*m_message).pop_front ();
            return part;
        }

        size_t size ()
        {
            return (*m_message).size ();
        }
    };

    static void log (const char *fmt, ...)
    {
		char date[50], buffer [256];

		struct tm *timeinfo;
        time_t t;

        time (&t);
        timeinfo = localtime (&t);

		if (!::strftime (date, sizeof(date), "%d/%b/%Y:%H:%M:%S %Z", timeinfo))
            return;

        va_list ap;
        va_start (ap, fmt);
        vsnprintf (buffer, 256, fmt, ap);
        va_end (ap);

        std::cerr << "[" << date << "] - " << buffer << std::endl;
    }
};

#endif