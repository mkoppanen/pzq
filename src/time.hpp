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

#ifndef PZQ_TIME_HPP
# define PZQ_TIME_HPP

#include <sys/time.h>

namespace pzq {

    inline
    uint64_t microsecond_timestamp ()
    {
        timeval tv;

        if (::gettimeofday (&tv, NULL)) {
            throw new std::runtime_error ("gettimeofday failed");
        }
        return static_cast<uint64_t> (tv.tv_sec) * 1000000ULL + static_cast<uint64_t> (tv.tv_usec);
    }
}

#endif
