
module JOR
  class Doc
    
    def self.paths(path,h)
      
      if h.class==Hash
        v = []
        h.each do |k,val|
          if JOR::Storage::SELECTORS.member?(k)
            puts "----->"
            return [{"path_to" => path, "obj" => h, "class" => h.class, "selector" => true}]
          else
            raise InvalidFieldName.new(k) if (k!="_id") && (k[0]=="_" || k[0]=="$")
            v << paths("#{path}/#{k}",val)
          end
        end
        return v.flatten
      else
        if h.class==Array
          v = []
          if h.size>0
            h.each do |item|
              v << paths("#{path}/[]",item)
           end
          else
            v << ["#{path}/[]"]
          end
          return v.flatten
        else
          return [{"path_to" => path, "obj" => h, "class" => h.class}]
        end
      end
    end

  end
end