
module JOR
  class Storage
  
    def initialize(redis = nil)
      redis = Redis.new() if redis.nil?
      @redis  = ::Redis::Namespace.new(:jor, :redis => redis)
      @redis
    end
    
    def insert2(doc)
      doc["_id"] = next_id if doc["_id"].nil?
      id = doc["_id"]
      enc_doc = JSON::generate(doc)
      
      @redis.multi do
        @redis.set("jor/docs/#{id}",enc_doc)
        build_idx_doc("0/",id,doc)
      end
    end
    
    
    def insert(doc)
      doc["_id"] = next_id if doc["_id"].nil?
      id = doc["_id"]
      enc_doc = JSON::generate(doc)
      
      paths = Doc.paths("$",doc)

      @redis.multi do 
        @redis.set("jor/docs/#{id}",enc_doc)
        paths.each do |path|
         add_index(path,id) 
        end
      end
      
      doc
    end
    
    def find(doc, options = {:all => false})
      # list of ids of the documents      
      ids = []
      
      ## if doc contains _id it ignores the rest of the doc's fields
      if !doc["_id"].nil?
        ids << doc["_id"]
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
      
      results = @redis.pipelined do
        ids.each do |id|
          @redis.get("jor/docs/#{id}")
        end
      end
      
      raise NoResults.new(doc) if results.nil? || results.size==0 

      results.map! { |item| JSON::parse(item) }
      
      if !options[:all].nil? && options[:all]==true
        results
      else
        results.first
      end
    end
    
    def redis
      @redis
    end
    
    protected
    
    def next_id
      @redis.incrby("jor/next_id",1)
    end
    
    def fetch_ids_by_index(path)
      
      if path["obj"].kind_of? String
        key = "jor/idx/#{path["path_to"]}/String/#{path["obj"]}"
        return @redis.smembers(key)
      else
        raise TypeNotSupported.new(value.class)
      end
        
    end
    
    def add_index(path, id)
      key = nil
            
      if path["obj"].kind_of? String
        key = "jor/idx/#{path["path_to"]}/String/#{path["obj"]}"
        @redis.sadd(key,id)
      elsif path["obj"].kind_of? Numeric
        key = "jor/idx/#{path["path_to"]}/Numeric"
        @redis.zadd(key,path["obj"],id)
      elsif path["obj"].kind_of? Time
        key = "jor/idx/#{path["path_to"]}/Time"
        @redis.zadd(key,path["obj"].to_i,id)
      else
        raise TypeNotSupported.new(value.class)
      end
      
      @redis.sadd("jor/sidx/#{id}",key) unless key.nil?
    end
  end
end
