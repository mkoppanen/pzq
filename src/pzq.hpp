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

namespace pzq 
{
    typedef std::list <boost::shared_ptr <zmq::message_t> > pzq_mp_message;

    typedef std::list <boost::shared_ptr <zmq::message_t> >::iterator pzq_mp_message_it;
}

#endif