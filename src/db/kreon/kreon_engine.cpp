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

extern "C"{
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <kreon_lib/scanner/scanner.h>
}

std::string Kreon_volume_name;

OP_NAMESPACE_BEGIN

    struct KreonIterData
    {
            Data ns;
            klc_scanner iter;
            KreonIterData()
                    : iter(NULL)
            {
            }
            ~KreonIterData()
            {
                if(NULL != iter && klc_is_valid(iter))
			        klc_close_scanner(iter);
            }
    };

    struct KreonLocalContext
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

    static ThreadLocal<KreonLocalContext> g_rocks_context;


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
        klc_close(m_db);
    }

    int KreonEngine::Backup(Context& ctx, const std::string& dir)
    {
        return ERR_NOTSUPPORTED;
    }

    int KreonEngine::Restore(Context& ctx, const std::string& dir)
    {
        return ERR_NOTSUPPORTED;
    }

    int KreonEngine::ReOpen()
    {
        if(m_db != NULL){
            printf("Cant clonse \n");
            Close();
        }
        
        int64_t size;
        int fd = open((const char*) Kreon_volume_name.c_str(), O_RDWR);
        if (fd == -1) {
            perror("open");
            exit(EXIT_FAILURE);                                            
        }
        size = lseek(fd, 0, SEEK_END);
        if (size == -1) {
            //log_fatal("failed to determine file size exiting...");
            perror("ioctl");
            exit(EXIT_FAILURE);                                                                
        }
        close(fd);
        //log_info("Size is %lld", size);
       
        klc_db_options db_option;
        db_option.volume_size = size;
        db_option.volume_name = strdup(Kreon_volume_name.c_str());
        db_option.db_name = strdup("test_ardb");
        db_option.volume_start = 0;
        db_option.create_flag = KLC_CREATE_DB;

	    
        m_db = klc_open(&db_option);
        if(m_db != NULL)
            return 0;
        else
            return 1;
    }

    int KreonEngine::Init(const std::string& dir, const std::string& conf)
    {
	m_dbdir = dir;
	Kreon_volume_name = (m_dbdir.substr(0,m_dbdir.size() - 5 ) + "kreon.dat").c_str();

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
        KreonLocalContext& kreons_ctx = g_rocks_context.GetValue();
        Buffer& encode_buffer = kreons_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        value.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        
        struct klc_key_value kv;
        
        kv.k.data = const_cast<char*>(encode_buffer.GetRawBuffer());
        kv.v.data = const_cast<char*>(encode_buffer.GetRawBuffer() + key_len);
        kv.k.size = key_len;
        kv.v.size = value_len;
       

        if(klc_put(m_db, &kv) == KLC_SUCCESS)
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
        KreonLocalContext& kreons_ctx = g_rocks_context.GetValue();
        std::string& valstr = kreons_ctx.GetStringCache();
        Buffer& key_encode_buffer = kreons_ctx.GetEncodeBuferCache();
        key.Encode(key_encode_buffer);
        
        struct klc_key k;

        k.size = key_encode_buffer.ReadableBytes();
        k.data = const_cast<char*>(key_encode_buffer.GetRawBuffer());
        char *key_buf = NULL;


        struct klc_value *v = NULL;
        if(klc_get(m_db , &k, &v) != KLC_SUCCESS)
            return ERR_ENTRY_NOT_EXIST;
        
        Buffer valBuffer( const_cast<char*>( v->data), 0, v->size);
        value.Decode(valBuffer, true);
        
        return 0;
        
    }


    int KreonEngine::Del(Context& ctx, const KeyObject& key)
    {
        KreonLocalContext& kreon_ctx = g_rocks_context.GetValue();
        Buffer& key_encode_buffer = kreon_ctx.GetEncodeBuferCache();
        key.Encode(key_encode_buffer);
        size_t key_len = key_encode_buffer.ReadableBytes();
        struct klc_key k;
        k.data = const_cast<char*>(key_encode_buffer.GetRawBuffer());

        if(klc_delete(m_db, &k) != KLC_SUCCESS)
            return 1;

        return 0;
    }

    int KreonEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
    {
       return 0;
    }

    bool KreonEngine::Exists(Context& ctx, const KeyObject& key,ValueObject& val)
    {
	    return 0 == Get(ctx,key,val);
    }

    Iterator* KreonEngine::Find(Context& ctx, const KeyObject& key)
    {
        KreonIterator* iter = NULL;
        NEW(iter, KreonIterator(this, key.GetNameSpace()));

        if (key.GetType() > 0)
        {
            if (!ctx.flags.iterate_multi_keys)
            {
                if (!ctx.flags.iterate_no_upperbound)
                {
                    KeyObject& upperbound_key = iter->IterateUpperBoundKey();
                    upperbound_key.SetNameSpace(key.GetNameSpace());
                    if (key.GetType() == KEY_META)
                    {
                        upperbound_key.SetType(KEY_END);
                    }else{
                        upperbound_key.SetType(key.GetType() + 1);
                    }
                    upperbound_key.SetKey(key.GetKey());
                    upperbound_key.CloneStringPart();
                }
            }
        }


       if (key.GetType() > 0)
        {
            iter->Jump(key);
        }
        else
        {
            iter->JumpToFirst();
        }
        return iter;

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

    void KreonIterator::SetIterator(klc_scanner iter)
    {
        //m_iter = iter;
        m_kreon_iter = iter;
    }

    bool KreonIterator::Valid()
    {
        if(m_kreon_iter != NULL && klc_is_valid(m_kreon_iter))
            return true;
        else 
            return false;

    }
    void KreonIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }
    void KreonIterator::CheckBound()
    {
        if(NULL != m_kreon_iter && m_iterate_upper_bound_key.GetType() > 0)
        {
            if(klc_is_valid(m_kreon_iter))
            {
                if (Key(false).Compare(m_iterate_upper_bound_key) >= 0)
                {
                    m_valid = false;
                }
            }
        }
    }
    void KreonIterator::Next()
    {
        ClearState();
        if (NULL == m_kreon_iter)
        {
            return;                                
        }
        klc_get_next(m_kreon_iter);
        CheckBound();
        
    }
    void KreonIterator::Prev()
    {
	ClearState();
	if(NULL == m_kreon_iter)
		return;

	klc_get_prev(m_kreon_iter);
	//CheckBound();
        
    }
    void KreonIterator::Jump(const KeyObject& next)
    {
        ClearState();

	if(m_kreon_iter != NULL && klc_is_valid(m_kreon_iter))
		klc_close_scanner(m_kreon_iter);

        KreonLocalContext& kreon_ctx = g_rocks_context.GetValue();
        Buffer& encode_buffer = kreon_ctx.GetEncodeBuferCache();
        next.Encode(encode_buffer, false);

        size_t key_len = encode_buffer.ReadableBytes();
        struct klc_key k;
        
        k.data = (char*)(encode_buffer.GetRawBuffer());
        k.size = key_len;

        m_kreon_iter = klc_init_scanner(m_engine->m_db, &k, KLC_GREATER_OR_EQUAL);

        CheckBound();
    }
    void KreonIterator::JumpToFirst()
    {
        ClearState();

        if( NULL == m_kreon_iter ){
            return;
        }
        m_kreon_iter = klc_init_scanner(m_engine->m_db, NULL , KLC_FETCH_FIRST);
        //klc_close_scanner(m_kreon_iter);
           //seek_to_first(m_engine->m_db,m_kreon_iter);
    }
    void KreonIterator::JumpToLast()
    {
	ClearState();
	if(NULL == m_kreon_iter)
		return;

	m_kreon_iter = klc_init_scanner(m_engine->m_db,NULL,KLC_FETCH_LAST);
    }

    KeyObject& KreonIterator::Key(bool clone_str)
    {
        if (m_key.GetType() > 0)
        {
            if (clone_str && m_key.GetKey().IsCStr())
            {
                m_key.CloneStringPart();
                                                        
            }
            return m_key;                                
        }

        struct klc_key keyptr;
        keyptr = klc_get_key(m_kreon_iter);
        Buffer kbuf(const_cast<char*>(keyptr.data), 0, keyptr.size); 
        m_key.Decode(kbuf, clone_str);
        m_key.SetNameSpace(m_ns);
        return m_key;
    }
    ValueObject& KreonIterator::Value(bool clone_str)
    {
        if(m_value.GetType() > 0)
        {
            if(clone_str){
                m_value.CloneStringPart();
            }
            return m_value;
        }

        struct klc_value valptr;
        valptr = klc_get_value(m_kreon_iter);
        Buffer vbuf(const_cast<char*>(valptr.data), 0 , valptr.size);
        m_value.Decode(vbuf,clone_str);
        return m_value;
    }
    void KreonIterator::Del()
    {

    }
    KreonIterator::~KreonIterator()
    {
        if(m_kreon_iter != NULL){
            klc_close_scanner(m_kreon_iter);
        }
            
    }

    Slice KreonIterator::RawKey()
    {
        if(m_kreon_iter == NULL)
            return NULL;

        struct klc_key k;
        k = klc_get_key(m_kreon_iter);
        return Slice((const char*) k.data , k.size);
        
    }
    Slice KreonIterator::RawValue()
    {
        if(m_kreon_iter == NULL)
            return NULL;

        struct klc_value v;
        v = klc_get_value(m_kreon_iter);
        return Slice((const char*) v.data, v.size);
    }

    
OP_NAMESPACE_END

