/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef REDIS_REPLY_HPP_
#define REDIS_REPLY_HPP_

#include "common.hpp"
#include "types.hpp"
#include <deque>
#include <vector>
#include <string>

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6
#define REDIS_REPLY_DOUBLE 1001


#define FIRST_CHUNK_FLAG  0x01
#define LAST_CHUNK_FLAG  0x02

namespace ardb
{
    namespace codec
    {

        enum ErrorCode
        {
            //STATUS_OK = 0,
            ERR_ENTRY_NOT_EXIST = -1000,
            ERR_INVALID_INTEGER_ARGS = -1001,
            ERR_INVALID_FLOAT_ARGS = -1002,
            ERR_INVALID_SYNTAX = -1003,
            ERR_AUTH_FAILED = -1004,
            ERR_NOTPERFORMED = -1005,
            ERR_STRING_EXCEED_LIMIT = -1006,
            ERR_NOSCRIPT = -1007,
            ERR_INVALID_ARGS = -3,
            ERR_INVALID_OPERATION = -4,
            ERR_INVALID_STR = -5,
            ERR_KEY_EXIST = -7,
            ERR_INVALID_TYPE = -8,
            ERR_OUTOFRANGE = -9,

            //ERR_ROCKS_kNotFound = -2001,
            ERR_ROCKS_kCorruption = -2002,
            ERR_ROCKS_kNotSupported = -2003,
            ERR_ROCKS_kInvalidArgument = -2004,
            ERR_ROCKS_kIOError = -2005,
            ERR_ROCKS_kMergeInProgress = -2006,
            ERR_ROCKS_kIncomplete = -2007,
            ERR_ROCKS_kShutdownInProgress = -2008,
            ERR_ROCKS_kTimedOut = -2009,
            ERR_ROCKS_kAborted = -2010,
            ERR_ROCKS_kBusy = -2011,
            ERR_ROCKS_kExpired = -2012,
            ERR_ROCKS_kTryAgain = -2013,
        };

        enum StatusCode
        {
            STATUS_OK = 1000,
            STATUS_PONG = 1001,
            STATUS_QUEUED = 1002,
        };

        struct RedisDumpFileChunk
        {
                int64 len;
                uint32 flag;
                std::string chunk;
                RedisDumpFileChunk() :
                        len(0), flag(0)
                {
                }
                bool IsLastChunk()
                {
                    return (flag & LAST_CHUNK_FLAG) == (LAST_CHUNK_FLAG);
                }
                bool IsFirstChunk()
                {
                    return (flag & FIRST_CHUNK_FLAG) == (FIRST_CHUNK_FLAG);
                }
        };

        class RedisReplyPool;
        struct RedisReply
        {
            private:

            public:
                int type;
                std::string str;

                /*
                 * If the type is REDIS_REPLY_STRING, and the str's length is large,
                 * the integer value also used to identify chunk state.
                 */
                int64_t integer;
                std::deque<RedisReply*>* elements;

                RedisReplyPool* pool;  //use object pool if reply is array with hundreds of elements
                RedisReply() :
                        type(REDIS_REPLY_NIL), integer(0), elements(NULL), pool(NULL)
                {
                }
                RedisReply(uint64 v) :
                        type(REDIS_REPLY_INTEGER), integer(v), elements(NULL), pool(NULL)
                {
                }
                RedisReply(double v) :
                        type(REDIS_REPLY_DOUBLE), integer(0), elements(NULL), pool(NULL)
                {
                }
                RedisReply(const std::string& v) :
                        type(REDIS_REPLY_STRING), str(v), integer(0), elements(NULL), pool(NULL)
                {
                }
                bool IsErr() const
                {
                	return type == REDIS_REPLY_ERROR;
                }
                const std::string& Error() const
                {
                	return str;
                }
                int64_t ErrCode() const
                {
                	return integer;
                }
                double GetDouble();
                void SetDouble(double v);
                void SetInteger(int64_t v)
                {
                    type = REDIS_REPLY_INTEGER;
                    integer = v;
                }
                void SetString(const Data& v)
                {
                    type = REDIS_REPLY_STRING;
                    v.ToString(str);
                }

                void SetString(const std::string& v)
                {
                    type = REDIS_REPLY_STRING;
                    str = v;
                }

                void SetErrCode(int err)
                {
                    type = REDIS_REPLY_ERROR;
                    integer = err;
                }
                void SetErrorReason(const std::string& reason)
                {
                    type = REDIS_REPLY_ERROR;
                    str = reason;
                }
                void SetStatusCode(int status)
                {
                    type = REDIS_REPLY_STATUS;
                    integer = status;
                }
                void SetPool(RedisReplyPool* pool);
                bool IsPooled()
                {
                    return pool != NULL;
                }
                RedisReply& AddMember(bool tail = true);
                void ReserveMember(size_t num);
                size_t MemberSize();
                RedisReply& MemberAt(uint32 i);
                void Clear();
                void Clone(const RedisReply& r)
                {
                    type = r.type;
                    integer = r.integer;
                    str = r.str;
                    if (r.elements != NULL && !r.elements->empty())
                    {
                        for (uint32 i = 0; i < r.elements->size(); i++)
                        {
                            RedisReply& rr = AddMember();
                            rr.Clone(*(r.elements->at(i)));
                        }
                    }
                }
                ~RedisReply();
        };

        class RedisReplyPool
        {
            private:
                uint32 m_max_size;
                uint32 m_cursor;
                std::vector<RedisReply> elements;
                std::deque<RedisReply> pending;
            public:
                RedisReplyPool(uint32 size = 5);
                void SetMaxSize(uint32 size);
                RedisReply& Allocate();
                void Clear();
        };

        void reply_status_string(int code, std::string& str);
        void reply_error_string(int code, std::string& str);
    }
}

#endif /* REDIS_REPLY_HPP_ */
