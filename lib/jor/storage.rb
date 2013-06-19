
module JOR
  class Storage
  
    NAMESPACE = "jor"
  
    SELECTORS = {
      :compare => ["$gt","$gte","$lt","$lte"],
      :sets => ["$in","$all"],
      :boolean => []
    }
    
    SELECTORS_ALL = SELECTORS.keys.inject([]) { |sel, element| sel | SELECTORS[element] } 
    
    def initialize(redis = nil)
      @redis = Redis.new() if @redis.nil?  
      @collections = {}
      reload_collections
    end

    def redis
      @redis
    end

    def collections
      @collections
    end
    
    def list_collections
      collections.keys
    end
    
    def create_collection(name, options = {:auto_increment => false})
      options = {:auto_increment => false}.merge(options.inject({}){|memo,(k,v)| memo[k.to_sym] = v; memo})
      raise CollectionNotValid.new(name) if self.respond_to?(name)
      is_new = redis.sadd("#{Storage::NAMESPACE}/collections",name)
      raise CollectionAlreadyExists.new(name) if is_new==false or is_new==0  
      redis.set("#{Storage::NAMESPACE}/collection/#{name}/auto-increment", options[:auto_increment])
      reload_collections
    end
    
    def destroy_collection(name)
      raise CollectionDoesNotExist.new(name) unless @collections[name]
      coll_to_be_removed = @collections[name]
      redis.srem("#{Storage::NAMESPACE}/collections",name)
      redis.del("#{Storage::NAMESPACE}/collection/#{name}/auto-increment")
      reload_collections
      coll_to_be_removed.delete({})
      raise Exception.new("CRITICAL! Destroying the collection left some documents hanging") if coll_to_be_removed.count()!=0
    end
    
    def destroy_all()
      collections.keys.each do |col|
        destroy_collection(col)
      end
    end
    
    def info
      res = {}
      ri = redis.info
      
      res["used_memory_in_redis"] = ri["used_memory"].to_i
      res["num_collections"] = collections.size
      
      res["collections"] = {}
      collections.each do |k, c|
        res["collections"][c.name] = {}
        res["collections"][c.name]["num_documents"] = c.count
        res["collections"][c.name]["auto_increment"] = c.auto_increment?
      end
      
      res
    end
    
    protected
    
    def reload_collections 
      coll = redis.smembers("#{Storage::NAMESPACE}/collections")
      tmp_collections = {}
      coll.each do |c|
        redis_auto_incr = redis.get("#{Storage::NAMESPACE}/collection/#{c}/auto-increment")
        redis_auto_incr=="true" ? auto_increment = true : auto_increment = false
        tmp_collections[c] = Collection.new(self, c, auto_increment)
      end
      @collections = tmp_collections
    end
    
    def method_missing(method)
      if !collections[method.to_s].nil?
        collections[method.to_s]
      else
        raise CollectionDoesNotExist.new(method.to_s)
      end
    end
       
  end
end
