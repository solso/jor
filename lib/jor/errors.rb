
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
  
  class NotInCollection < Error
    def initialize
      super %(The current collection is undefined)
    end
  end
  
  class CollectionDoesNotExist < Error
    def initialize(str)
      super %(Collection "#{str}" does not exist)
    end
  end

  class CollectionAlreadyExists < Error
    def initialize(str)
      super %(Collection "#{str}" already exists)
    end
  end

  class CollectionNotValid < Error
    def initialize(str)
      super %(Collection "#{str}" is not a valid name, might be reserver)
    end
  end
  
  

end