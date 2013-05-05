
module JOR  
  class Collections < Array
    
    def initialize(storage)
      @collections = {}
      @storage = storage
      reload_collections
    end
    
    def storage 
      @storage
    end
    
    def create(name)
      raise CollectionNotValid.new(name) if @storage.respond_to?(name)
      is_new = storage.redis.sadd("#{Storage::NAMESPACE}/collections",name)
      raise CollectionAlreadyExists.new(name) if is_new==false or is_new==0
      reload_collections
    end
    
    def destroy(name)
      raise CollectionDoesNotExist.new(name) unless @collections[name]
      coll_to_be_removed = @collections[name]
      storage.redis.srem("#{Storage::NAMESPACE}/collections",name)
      reload_collections
      coll_to_be_removed.delete({})
      raise Exception.new("CRITICAL! Destroying the collection left some documents hanging") if coll_to_be_removed.count()!=0
    end
    
    def destroy_all()
      list.each do |col|
        destroy(col)
      end
    end
    
    def list
      @collections.keys
    end
    
    def collections
      @collections
    end
       
    def reload_collections 
      coll = storage.redis.smembers("#{Storage::NAMESPACE}/collections")
      tmp_collections = {}
      coll.each do |c|
        tmp_collections[c] = Collection.new(storage,c)
      end
      @collections = tmp_collections
    end
    
  end
end