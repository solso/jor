
module JOR
  class Error < RuntimeError
  end
  
  class NoResults < Error
    def initialize(doc)
      super %(no results found for "#{doc}")
    end
  end
  
  class TypeNotSupported < Error
    def initialize(class_name)
      super %(Type #{class_name} not supported")
    end
  end
  
end