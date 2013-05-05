
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
      @collections = Collections.new(self)
    end

    def redis
      @redis
    end

    def collections
      @collections
    end
    
    def method_missing(method)
      if !@collections.collections[method.to_s].nil?
        @collections.collections[method.to_s]
      else
        raise CollectionDoesNotExist.new(method.to_s)
      end
    end
       
  end
end
