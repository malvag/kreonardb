/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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

#ifndef KREON_HPP_
#define KREON_HPP_


#include "common/common.hpp"
#include "thread/spin_rwlock.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/thread_local.hpp"
#include "db/engine.hpp"
#include <vector>
//#include <sparsehash/dense_hash_map>
#include <memory>
extern "C"{
#include <kreon_lib/include/kreon.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
}


OP_NAMESPACE_BEGIN

    struct KreonIterData;
    class KreonEngine;
    class KreonIterator: public Iterator
    {
        private:
            Data m_ns;
            KeyObject m_key;
            ValueObject m_value;
            KreonEngine* m_engine;
            //rocksdb::ColumnFamilyHandle* m_cf;
            KreonIterData* m_iter;
            klc_scanner m_kreon_iter;
            KeyObject m_iterate_upper_bound_key;
            bool m_valid;
            void ClearState();
            void CheckBound();
        public:
            KreonIterator(KreonEngine* engine,const Data& ns) :
                    m_ns(ns), m_engine(engine),  m_iter(NULL),m_valid(true)
            {
                m_kreon_iter = NULL;
            }
            void MarkValid(bool valid)
            {
                m_valid = valid;
            }
            void SetIterator(KreonIterData* iter);
            KeyObject& IterateUpperBoundKey()
            {
                return m_iterate_upper_bound_key;
            }
            bool Valid();
            void Next();
            void Prev();
            void Jump(const KeyObject& next);
            void JumpToFirst();
            void JumpToLast();
            KeyObject& Key(bool clone_str);
            ValueObject& Value(bool clone_str);
            void Del();
            Slice RawKey();
            Slice RawValue();
            ~KreonIterator();
    };

    class RocksDBCompactionFilter;
    class KreonEngine: public Engine
    {
        private:
            //typedef std::shared_ptr<rocksdb::ColumnFamilyHandle> ColumnFamilyHandlePtr;
            //typedef TreeMap<Data, ColumnFamilyHandlePtr>::Type ColumnFamilyHandleTable;
            //typedef TreeMap<uint32_t, Data>::Type ColumnFamilyHandleIDTable;
            klc_handle m_db;

            //rocksdb::Options m_options;
            std::string m_dbdir;
            //ColumnFamilyHandleTable m_handlers;
            SpinRWLock m_lock;
            ThreadMutex m_backup_lock;

            //ColumnFamilyHandlePtr GetColumnFamilyHandle(Context& ctx, const Data& name, bool create_if_noexist);
            friend class KreonIterator;
            int ReOpen();
            void Close();
        public:
            KreonEngine();
            ~KreonEngine();
            int Init(const std::string& dir, const std::string& options);
            int Repair(const std::string& dir);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs);
            int Del(Context& ctx, const KeyObject& key);
            int Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args);
            bool Exists(Context& ctx, const KeyObject& key,ValueObject& val);
            int BeginWriteBatch(Context& ctx);
            int CommitWriteBatch(Context& ctx);
            int DiscardWriteBatch(Context& ctx);
            int Compact(Context& ctx, const KeyObject& start, const KeyObject& end);
            int ListNameSpaces(Context& ctx, DataArray& nss);
            int DropNameSpace(Context& ctx, const Data& ns);
            void Stats(Context& ctx, std::string& str);
            int64_t EstimateKeysNum(Context& ctx, const Data& ns);
            Iterator* Find(Context& ctx, const KeyObject& key);
            const std::string GetErrorReason(int err);
            int Backup(Context& ctx, const std::string& dir);
            int Restore(Context& ctx, const std::string& dir);
            const FeatureSet GetFeatureSet();
            int MaxOpenFiles();
    };

OP_NAMESPACE_END
#endif /* SRC_KREON_HPP_ */
