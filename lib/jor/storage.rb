
module JOR
  class Storage
  
    SELECTORS = {
      :compare => ["$gt","$gte","$lt","$lte"],
      :sets => ["$in","$all"],
      :boolean => []
    }
    
    SELECTORS_ALL = SELECTORS.keys.inject([]) { |sel, element| sel | SELECTORS[element] } 
    
    def initialize(redis = nil)
      redis = Redis.new() if redis.nil?
      @redis  = Redis::Namespace.new(:jor, :redis => redis)
      @redis
    end
    
    def insert(docs, options = {})
      
      docs.is_a?(Array) ? docs_list = docs : docs_list = [docs]
    
      docs_list.each do |doc|  
        doc["_id"] = next_id if doc["_id"].nil?
        id = doc["_id"]
        encd = JSON::generate(doc)
        paths = Doc.paths("$",doc)

        redis.multi do 
          redis.set("jor/docs/#{id}",encd)
          redis.sadd("jor/sdocs",id)
          paths.each do |path|
            add_index(path,id) 
          end
        end
      end
      
      docs
    end
    
    def delete(doc, options ={})
      ids = find(doc, {:only_id => true})
      ids.each do |id|
        delete_by_id(id)
      end
      ids.size
    end
        
    def count
      redis.scard("jor/sdocs")
    end
    
    def find(doc, options = {:all => false})
      # list of ids of the documents      
      ids = []
      
      ## if doc contains _id it ignores the rest of the doc's fields
      if !doc["_id"].nil? && !doc["_id"].kind_of?(Hash)
        ids << doc["_id"]
        return [] if options[:only_id]==true && redis.get("jor/docs/#{ids.first}").nil?
      elsif (doc == {})
        ids = redis.smembers("jor/sdocs")
      else
        paths = Doc.paths("$",doc)
   
        ## for now, consider all logical and
        paths.each_with_index do |path, i|
          tmp_res = fetch_ids_by_index(path)
          if i==0
            ids = tmp_res
          else
            ids = ids & tmp_res
          end
        end
      end
         
      ## we have now the list of id's the match the criteria, we can
      ## fetch the docs by id now. Pagination (cursor) should go here.
      ## also, consider returning the list of id's as options
      
      return [] if ids.nil? || ids.size==0
      
      return ids if (options[:only_id]==true)
        
      results = redis.pipelined do
        ids.each do |id|
          redis.get("jor/docs/#{id}")
        end
      end
      
      return [] if results.nil? || results.size==0  
      #raise NoResults.new(doc) if results.nil? || results.size==0
      results.map! { |item| JSON::parse(item) }
      
      return results
    end
    
    def redis
      @redis
    end
    
    protected
    
    def next_id
      redis.incrby("jor/next_id",1)
    end
    
    def find_docs(doc)
      return [doc["_id"]] if !doc["_id"].nil?
    
      ids = []
      paths = Doc.paths("$",doc)
      
      ## for now, consider all logical and
      paths.each_with_index do |path, i|
        tmp_res = fetch_ids_by_index(path)
        if i==0
          ids = tmp_res
        else
          ids = ids & tmp_res
        end
      end
      ids
    end

    def check_selectors(sel)
      Storage::SELECTORS.each do |type, set|        
        istrue = true
        sel.each do |s|
          istrue = istrue && set.member?(s)
        end
        return type if istrue
      end
      raise IncompatibleSelectors.new(selectors)      
    end

    def find_type(obj)
      [String, Numeric, Time].each do |type|
        return type if obj.kind_of? type
      end
      raise TypeNotSupported.new(obj.class)
    end

    def fetch_ids_by_index(path)
      
      if path["selector"]==true
        ## there is a selector
        
        type = check_selectors(path["obj"].keys)
        
        if type == :compare
          key = idx_key(path["path_to"], find_type(path["obj"].values.first))
        
          rmin = "-inf"
          rmin = path["obj"]["$gte"] unless path["obj"]["$gte"].nil?
          rmin = "(#{path["obj"]["$gt"]}" unless path["obj"]["$gt"].nil?
                    
          rmax = "+inf"
          rmax = path["obj"]["$lte"] unless path["obj"]["$lte"].nil?
          rmax = "(#{path["obj"]["$lt"]}" unless path["obj"]["$lt"].nil?
          
          ##ZRANGEBYSCORE zset (5 (10 : 5 < x < 10          
          return redis.zrangebyscore(key,rmin,rmax)
          
        elsif type == :sets
          
          if path["obj"]["$in"]
            target = path["obj"]["$in"]
            join_set = []
            target.each do |item|
              join_set = join_set | redis.smembers(idx_key(path["path_to"], find_type(item), item))
            end
            return join_set
          elsif path["obj"]["$all"]
            join_set = []
            target = path["obj"]["$all"]
            target.each do |item|
              if join_set.size==0
                join_set = redis.smembers(idx_key(path["path_to"], find_type(item), item))
              else
                join_set = join_set & redis.smembers(idx_key(path["path_to"], find_type(item), item))
              end
              return [] if (join_set.nil? || join_set.size==0)
            end
            return join_set
          else
            ## should be an error
          end  
          
        else
            
        end   
      end
      
      if path["obj"].kind_of? String
        return redis.smembers(idx_key(path["path_to"], String, path["obj"]))
      elsif path["obj"].kind_of? Numeric
        return redis.smembers(idx_key(path["path_to"], Numeric, path["obj"]))
      elsif path["obj"].kind_of? Time
        return []
      else
        raise TypeNotSupported.new(value.class)
      end
        
    end
    
    def delete_by_id(id)
      
      indexes = redis.smembers(idx_set_key(id))
      
      redis.pipelined do
        indexes.each do |index|
  
          v = index.split("_")
          key = v[0..v.size-2].join("_")
          if v.last=="srem"
            redis.srem(key, id)
          elsif v.last=="zrem"
            redis.zrem(key, id)
          end
        end
      
        redis.del(idx_set_key(id))
        redis.srem("jor/sdocs",id)
        redis.del("jor/docs/#{id}")
        
      end
      
    end
    
    
    def add_index(path, id)
      if path["obj"].kind_of?(String)
        key = idx_key(path["path_to"], String, path["obj"])
        redis.sadd(key, id)
        redis.sadd(idx_set_key(id), "#{key}_srem")
      elsif path["obj"].kind_of?(Numeric)
        key = idx_key(path["path_to"], Numeric, path["obj"])
        redis.sadd(key, id)
        redis.sadd(idx_set_key(id), "#{key}_zrem")
        key = idx_key(path["path_to"], Numeric)
        redis.zadd(key, path["obj"], id)
        redis.sadd(idx_set_key(id), "#{key}_zrem")
      elsif path["obj"].kind_of?(Time)
        key = idx_key(path["path_to"], Time, path["obj"])
        redis.sadd(key, id)
        redis.sadd(idx_set_key(id), key)
        key = idx_key(path["path_to"], Time)
        redis.zadd(key,path["obj"], id)
        redis.sadd(idx_set_key(id), key)
      else
        raise TypeNotSupported.new(value.class)
      end
    end
    
    def idx_key(path_to, type, obj = nil)
      tmp = ""
      tmp = "/#{obj}" unless obj.nil?
      "jor/idx/#{path_to}/#{type}#{tmp}"
    end
    
    def idx_set_key(id)
      "jor/sidx/#{id}"
    end

  end
end
