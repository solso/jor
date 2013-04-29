
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
      super %(Type #{class_name} not supported)
    end
  end
  
  class InvalidFieldName < Error
    def initialize(field)
      super %(Invalid character in field name "#{field}". Cannot start with '_' or '$')
    end
  end
  
  class IncompatibleSelectors < Error
    def initialize(str)
      super %(Incompatible selectors in "#{str}". They must be grouped like this #{Storage::SELECTORS})
    end
  end

end