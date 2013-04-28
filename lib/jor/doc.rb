
module JOR
  class Doc
    
    def self.paths(path,h)
      
      if h.class==Hash
        v = []
        h.each do |k,val|
          v << paths("#{path}/#{k}",val)
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