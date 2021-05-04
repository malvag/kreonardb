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
#include "kreon_engine.hpp"
#include "thread/lock_guard.hpp"
#include "thread/spin_mutex_lock.hpp"
#include "db/db.hpp"
#include "util/string_helper.hpp"

OP_NAMESPACE_BEGIN

    struct KreonIterData
    {
            Data ns;
            struct Kreoniterator* iter;
            KreonIterData()
                    : iter(NULL)
            {
            }
            ~KreonIterData()
            {
                DELETE(iter);
            }
    };

    struct KreonDBLocalContext
    {
            Buffer encode_buffer_cache;
            std::string string_cache;
            std::vector<std::string> multi_string_cache;
            typedef TreeMap<int, int>::Type ErrMap;
            ErrMap err_map;
            Buffer& GetEncodeBuferCache()
            {
                encode_buffer_cache.Clear();
                return encode_buffer_cache;
            }
            std::string& GetStringCache()
            {
                string_cache.clear();
                return string_cache;
            }
            std::vector<string>& GetMultiStringCache()
            {
                return multi_string_cache;
            }
    };

    static ThreadLocal<KreonDBLocalContext> g_rocks_context;


    //static ThreadLocal<KreonLocalContext> g_rocks_context;
    //static RocksIteratorCache g_iter_cache;

    static inline int Kreon_err(int err)
    {
        return err + STORAGE_ENGINE_ERR_OFFSET;
    }

    KreonEngine::KreonEngine()
            : m_db(NULL)
    {
    }

    KreonEngine::~KreonEngine()
    {
        
        Close();
    }

    void KreonEngine::Close()
    {
        //db_close(m_db);
    }

    int KreonEngine::Backup(Context& ctx, const std::string& dir)
    {
        printf("BACKUP\n");
        return ERR_NOTSUPPORTED;
    }

    int KreonEngine::Restore(Context& ctx, const std::string& dir)
    {
        printf("RESTORE");
        return ERR_NOTSUPPORTED;
    }

    int KreonEngine::ReOpen()
    {
        if(m_db != NULL){
            printf("Cant clonse \n");
            Close();
        }
        
        char *volume_name = strdup("/var/ardb/kreon.dat");
        char *db_name = strdup("test_ardb");
	    int64_t device_size;
	    FD = open(volume_name, O_RDWR);
	    if (ioctl(FD, BLKGETSIZE64, &device_size) == -1) {
		device_size = lseek(FD, 0, SEEK_END);
		    if (device_size == -1) {
	    		// log_fatal("failed to determine volume size exiting...");
	    		perror("ioctl");
	    		exit(EXIT_FAILURE);
	    	}
	    }
	    m_db = db_open(volume_name, (uint64_t)0, (uint64_t)device_size, db_name, CREATE_DB);
        if(m_db != NULL)
            return 0;
        else
            return 1;
    }

    int KreonEngine::Init(const std::string& dir, const std::string& conf)
    {
        return ReOpen();
    }


    int KreonEngine::Repair(const std::string& dir)
    {
        return 0;
    }


    int KreonEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        return 0;
    }

    int KreonEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        printf("PUT");
        KreonDBLocalContext& kreons_ctx = g_rocks_context.GetValue();
        Buffer& encode_buffer = kreons_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        value.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        
        void* key_data, * value_data;
        uint32_t key_size,value_size;

        key_data = const_cast<char*>(encode_buffer.GetRawBuffer());
        key_size = key_len;
        value_data = const_cast<char*>(encode_buffer.GetRawBuffer() + key_len);
        value_size = value_len;

        //Doesnt support batches (transcations) yet.

        //prosexe ta return
        if(insert_key_value(m_db, (void*) key_data , (void*) value_data , key_size , value_size) == SUCCESS)
            return 0;
        else
            return 1;

    }
    int KreonEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
    {
        return 0;
    }
    int KreonEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        return 0;
    }


    int KreonEngine::Del(Context& ctx, const KeyObject& key)
    {
        return 0;
    }

    int KreonEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
    {
       return 0;
    }

    bool KreonEngine::Exists(Context& ctx, const KeyObject& key,ValueObject& val)
    {
       return 0;
    }

    Iterator* KreonEngine::Find(Context& ctx, const KeyObject& key)
    {
        printf("Find\n");
        KreonIterator* iter = NULL;
        NEW(iter, KreonIterator(this, key.GetNameSpace()));

        KreonIterData* kreonsiter = NULL;
        if (NULL == kreonsiter)
        {
            NEW(kreonsiter, KreonIterData);
            kreonsiter->iter = (struct Kreoniterator*) malloc(sizeof(struct Kreoniterator));
            
            iter->SetIterator(kreonsiter);

            if (key.GetType() > 0)
            {
                iter->Jump(key);
            }
            else
            {
                iter->JumpToFirst();
            }
            return iter;

            //g_iter_cache.AddRunningIter(rocksiter);
        }


        return NULL;
    }

    int KreonEngine::BeginWriteBatch(Context& ctx)
    {
        return 0;
    }
    int KreonEngine::CommitWriteBatch(Context& ctx)
    {
        return 0;
    }
    int KreonEngine::DiscardWriteBatch(Context& ctx)
    {
        return 0;
    }

    int KreonEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        return 0;
    }

    int KreonEngine::ListNameSpaces(Context& ctx, DataArray& nss)
    {
        return 0;
    }

    const std::string KreonEngine::GetErrorReason(int err)
    {
        return "";
    }

    int KreonEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        return 0;
    }

    int64_t KreonEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        return 0;
    }

    

    void KreonEngine::Stats(Context& ctx, std::string& all)
    {
    }

    const FeatureSet KreonEngine::GetFeatureSet()
    {
        FeatureSet features;
        features.support_compactfilter = 1;
        features.support_namespace = 1;
        features.support_merge = 1;
        features.support_backup = 0;
        features.support_delete_range = 0;
        return features;
    }

    int KreonEngine::MaxOpenFiles()
    {
        return 1;
    }

    void KreonIterator::SetIterator(KreonIterData* iter)
    {
        m_iter = iter;
        m_kreon_iter = m_iter->iter;
    }

    bool KreonIterator::Valid()
    {
        return true;
    }
    void KreonIterator::ClearState()
    {
        
    }
    void KreonIterator::CheckBound()
    {
        
    }
    void KreonIterator::Next()
    {
        
    }
    void KreonIterator::Prev()
    {
        
    }
    void KreonIterator::Jump(const KeyObject& next)
    {
        
        Seek(m_engine->m_db,NULL, m_kreon_iter);
        
    }
    void KreonIterator::JumpToFirst()
    {
        seek_to_first(m_engine->m_db,m_kreon_iter);
    }
    void KreonIterator::JumpToLast()
    {
       
    }

    KeyObject& KreonIterator::Key(bool clone_str)
    {
        KeyObject* tmp = new KeyObject();
        return *tmp;
    }
    ValueObject& KreonIterator::Value(bool clone_str)
    {
        ValueObject* tmp = new ValueObject();
        return *tmp;
    }
    void KreonIterator::Del()
    {

    }
    KreonIterator::~KreonIterator()
    {

    }

    Slice KreonIterator::RawKey()
    {
        return NULL;
    }
    Slice KreonIterator::RawValue()
    {
        return NULL;
    }

    
OP_NAMESPACE_END

