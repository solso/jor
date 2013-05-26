require File.expand_path(File.dirname(__FILE__) + '/../test_helper')

class CollectionTest < Test::Unit::TestCase

  def setup 
    redis = Redis.new(:db => 9)
    @jor = JOR::Storage.new(redis)
    list = @jor.redis.keys("*")
    raise "Cannot run the tests safely!! The test DB (:db => 9) is not empty, and the test might flush the data. Stopping." if list.size>0
    @jor.create_collection("test")
  end
  
  def teardown
    @jor.test.redis.flushdb()
  end
  
  def test_basic_insert_and_find_path
    doc1 = create_sample_doc_restaurant({"_id" => 1})
    @jor.test.insert(doc1)
    
    doc2 = create_sample_doc_restaurant({"_id" => 2})
    @jor.test.insert(doc2)
    
    doc3 = create_sample_doc_restaurant({"_id" => 3})
    @jor.test.insert(doc3)
    
    assert_equal 3, @jor.test.count()
    assert_equal 3, @jor.test.find({}).size
    
    assert_equal doc1.to_json, @jor.test.find({"_id" => 1}).first.to_json
    assert_equal doc2.to_json, @jor.test.find({"_id" => 2}).first.to_json
    assert_equal doc3.to_json, @jor.test.find({"_id" => 3}).first.to_json
  end
  
  def test_bulk_insert
    
    sample_docs = []
    10.times do |i|
      sample_docs << @jor.test.insert({"_id" => i, "name" => "foo_#{i}"})
    end
    
    @jor.test.insert(sample_docs)
    
    assert_equal 10, @jor.test.count()
    
    docs = @jor.test.find({})
    10.times do |i|
      assert_equal sample_docs[i].to_json, docs[i].to_json
    end
    
  end
  
  def test_delete
    ## MUST ALSO TEST THAT NO KEYS ARE LEFT HANGING 
    sample_docs = []
    10.times do |i|
      sample_docs << @jor.test.insert({"_id" => i, "name" => "foo_#{i}", "foo" => "bar", "year" => 2000+i })
    end
    
    @jor.test.insert(sample_docs)
    assert_equal 10, @jor.test.count()
    
    assert_equal 0, @jor.test.delete({"_id" => 42})
    assert_equal 10, @jor.test.count()
    
    assert_equal 0, @jor.test.delete({"foo" => "not_bar"})
    assert_equal 10, @jor.test.count()
        
    assert_equal 1, @jor.test.delete({"_id" => 0})      
    assert_equal 9, @jor.test.count()
    
    assert_equal 3, @jor.test.delete({"year" => { "$lt" => 2004 }})
    assert_equal 6, @jor.test.count()
    
    assert_equal 6, @jor.test.delete({"foo" => "bar"})     
    assert_equal 0, @jor.test.count()      
  end
  
  def test_find_exact_string
    
    sample_docs = []
    10.times do |i|
      sample_docs << @jor.test.insert({"_id" => i, "name" => "foo_#{i}"})
    end

    doc = @jor.test.find({"name" => "foo_5"}).first
    assert_equal sample_docs[5].to_json, doc.to_json
    
    doc = @jor.test.find({"name" => "foo_7"}).first
    assert_equal sample_docs[7].to_json, doc.to_json
    
  end
  
  def test_find_by_comparison_selector
    
    sample_docs = []
    ## years from 2000 to 2009
    10.times do |i|
      sample_docs << @jor.test.insert({"_id" => i, "name" => "foo_#{i}", "year" => 2000+i})
    end
    
    doc = @jor.test.find({"year" => 2005}).first
    assert_equal sample_docs[5].to_json, doc.to_json
    
    doc = @jor.test.find({"year" => { "$lt" => 2005 }})
    assert_equal 5, doc.size
    assert_equal sample_docs[0].to_json, doc.first.to_json
    assert_equal sample_docs[4].to_json, doc.last.to_json
    
    doc = @jor.test.find({"year" => { "$lte" => 2005 }})
    assert_equal 6, doc.size
    assert_equal sample_docs[0].to_json, doc.first.to_json
    assert_equal sample_docs[5].to_json, doc.last.to_json
    
    doc = @jor.test.find({"year" => { "$gt" => 2007 }})
    assert_equal 2, doc.size
    assert_equal sample_docs[8].to_json, doc.first.to_json
    assert_equal sample_docs[9].to_json, doc.last.to_json
    
    doc = @jor.test.find({"year" => { "$gte" => 2007 }})
    assert_equal 3, doc.size
    assert_equal sample_docs[7].to_json, doc.first.to_json
    assert_equal sample_docs[9].to_json, doc.last.to_json
    
    doc = @jor.test.find({"year" => { "$gte" => 2003, "$lt" => 2005 }})
    assert_equal 2, doc.size
    assert_equal sample_docs[3].to_json, doc.first.to_json
    assert_equal sample_docs[4].to_json, doc.last.to_json

    doc = @jor.test.find({"year" => { "$gt" => 2003, "$lt" => 9999 }})
    assert_equal 6, doc.size
    assert_equal sample_docs[4].to_json, doc.first.to_json
    assert_equal sample_docs[9].to_json, doc.last.to_json
     
  end
  
  def test_find_by_comparison_combined
    
    sample_docs = []
    ## years from 2000 to 2009
    10.times do |i|
      sample_docs << @jor.test.insert({"_id" => i, "year" => 2000+i, "desc" => "bar", "nested" => {"name" => "foo_#{i}", "quantity" => i.to_f}})
    end

    docs = @jor.test.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "nested" => {"quantity" => {"$lt" => 1.0}}})
    assert_equal 0, docs.size
    assert_equal [], docs
    
    docs = @jor.test.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "nested" => {"quantity" => {"$lt" => 4.0}}})
    assert_equal 2, docs.size
    assert_equal sample_docs[2].to_json, docs.first.to_json
    assert_equal sample_docs[3].to_json, docs.last.to_json
    
    docs = @jor.test.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "nested" => {"name" => "foo_#{4}", "quantity" => {"$lte" => 4.0}}})
    assert_equal 1, docs.size
    assert_equal sample_docs[4].to_json, docs.first.to_json
    
    docs = @jor.test.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "desc" => "bar", "nested" => {"name" => "foo_#{4}", "quantity" => {"$lte" => 4.0}}})
    assert_equal 1, docs.size
    assert_equal sample_docs[4].to_json, docs.first.to_json
        
    docs = @jor.test.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "desc" => "NOT_bar", "nested" => {"name" => "foo_#{4}", "quantity" => {"$lte" => 4.0}}})
    assert_equal 0, docs.size
        
  end
  
  def test_find_by_set_selector
    
    sample_docs = []
    ## years from 2000 to 2009
    10.times do |i|
      sample_docs << @jor.test.insert({"_id" => i, "name" => "foo_#{i}", "nested" => { "year" => 2000+i, "pair" => ((i%2)==0 ? "even" : "odd")} })
    end
    
    docs = @jor.test.find({"_id" => {"$in" => []}})
    assert_equal 0, docs.size
    
    docs = @jor.test.find({"_id" => {"$in" => [42]}})
    assert_equal 0, docs.size
    
    docs = @jor.test.find({"_id" => {"$in" => [8]}})
    assert_equal 1, docs.size
    assert_equal sample_docs[8].to_json, docs.first.to_json
    
    docs = @jor.test.find({"_id" => {"$all" => [8]}})
    assert_equal 1, docs.size
    assert_equal sample_docs[8].to_json, docs.first.to_json
                
    docs = @jor.test.find({"_id" => {"$in" => [1, 2, 3, 4, 42]}})
    assert_equal 4, docs.size
    assert_equal sample_docs[1].to_json, docs.first.to_json
    assert_equal sample_docs[4].to_json, docs.last.to_json
    
    docs = @jor.test.find({"_id" => {"$all" => [1, 2, 3, 4, 42]}})
    assert_equal 0, docs.size
            
    docs = @jor.test.find({"name" => {"$in" => ["foo_42"]}})
    assert_equal 0, docs.size

    docs = @jor.test.find({"name" => {"$all" => ["foo_42"]}})
    assert_equal 0, docs.size
        
    docs = @jor.test.find({"name" => {"$in" => []}})
    assert_equal 0, docs.size
    
    docs = @jor.test.find({"name" => {"$all" => []}})
    assert_equal 0, docs.size
    
    docs = @jor.test.find({"name" => {"$in" => ["foo_7", "foo_8", "foo_42"]}})
    assert_equal 2, docs.size
    assert_equal sample_docs[7].to_json, docs.first.to_json
    assert_equal sample_docs[8].to_json, docs.last.to_json
    
    docs = @jor.test.find({"nested" => {"pair" => { "$in" => ["even", "odd"]}}})
    assert_equal 10, docs.size
    assert_equal sample_docs[0].to_json, docs.first.to_json
    assert_equal sample_docs[9].to_json, docs.last.to_json
    
    docs = @jor.test.find({"nested" => {"pair" => { "$all" => ["even", "odd"]}}})
    assert_equal 0, docs.size
        
    docs = @jor.test.find({"nested" => {"pair" => { "$in" => ["even"]}}})
    assert_equal 5, docs.size
    assert_equal sample_docs[0].to_json, docs.first.to_json
    assert_equal sample_docs[8].to_json, docs.last.to_json
    
    docs = @jor.test.find({"nested" => {"pair" => { "$all" => ["even"]}}})
    assert_equal 5, docs.size
    assert_equal sample_docs[0].to_json, docs.first.to_json
    assert_equal sample_docs[8].to_json, docs.last.to_json
    
    docs = @jor.test.find({"nested" => {"pair" => { "$in" => ["even", "fake"]}}})
    assert_equal 5, docs.size
    assert_equal sample_docs[0].to_json, docs.first.to_json
    assert_equal sample_docs[8].to_json, docs.last.to_json
        
    docs = @jor.test.find({"nested" => {"pair" => { "$all" => ["even", "fake"]}}})
    assert_equal 0, docs.size
    
  end
  
  def test_playing_with_find_options
    
    n = (JOR::Collection::DEFAULT_OPTIONS[:max_documents]+100)
    
    n.times do |i|
      doc = create_sample_doc_restaurant({"_id" => i})
      @jor.test.insert(doc)
    end
    
    ## testing max_documents
    
    docs = @jor.test.find({})
        
    assert_equal JOR::Collection::DEFAULT_OPTIONS[:max_documents], docs.size
    assert_equal 0, docs.first["_id"]
    assert_equal JOR::Collection::DEFAULT_OPTIONS[:max_documents]-1, docs.last["_id"]
    
    docs = @jor.test.find({},{:max_documents => 20})
    assert_equal 20, docs.size    
        
    docs = @jor.test.find({},{:max_documents => -1})
    assert_equal n, docs.size
    assert_equal 0, docs.first["_id"]
    assert_equal n-1, docs.last["_id"]

    ## testing only_ids
    
    docs = @jor.test.find({},{:only_ids => true, :max_documents => -1})
    assert_equal n, docs.size
    assert_equal 0, docs.first
    assert_equal n-1, docs.last
    
    docs = @jor.test.find({},{:only_ids => true})
    assert_equal JOR::Collection::DEFAULT_OPTIONS[:max_documents], docs.size
    assert_equal 0, docs.first
    assert_equal JOR::Collection::DEFAULT_OPTIONS[:max_documents]-1, docs.last
    
    ## testing reversed
    
    docs = @jor.test.find({},{:only_ids => true, :max_documents => -1, :reversed => true})
    assert_equal n, docs.size
    assert_equal n-1, docs.first
    assert_equal 0, docs.last

    docs = @jor.test.find({},{:only_ids => true, :reversed => true})
    assert_equal JOR::Collection::DEFAULT_OPTIONS[:max_documents], docs.size
    assert_equal n-1, docs.first
    assert_equal n-JOR::Collection::DEFAULT_OPTIONS[:max_documents], docs.last
    
    ## encoded false
    
    docs = @jor.test.find({},{:raw => true, :reversed => true})
    assert_equal JOR::Collection::DEFAULT_OPTIONS[:max_documents], docs.size
    assert_equal String, docs.first.class
    assert_equal String, docs.last.class
    assert_equal n-1, JSON::parse(docs.first)["_id"]
    assert_equal n-JOR::Collection::DEFAULT_OPTIONS[:max_documents], JSON::parse(docs.last)["_id"]
    
  end
  
  def test_exclude_indexes
    
    assert_raise JOR::FieldIdCannotBeExcludedFromIndex do
      @jor.test.insert(create_sample_doc_restaurant({"_id" => 1}), 
        {:excluded_fields_to_index => {"_id" => true}})
    end
    
    @jor.test.insert(create_sample_doc_restaurant({"_id" => 1}), 
      {:excluded_fields_to_index => {"description" => true}})
 
    @jor.test.insert(create_sample_doc_restaurant({"_id" => 42}), 
      {:excluded_fields_to_index => {}})

    res = @jor.test.find({},{:reversed => true})
    assert_equal 2, res.size
    assert_equal "very long description that we might not want to index", res.first["description"]
    assert_equal 42, res.first["_id"]
    assert_equal "very long description that we might not want to index", res.last["description"]
    assert_equal 1, res.last["_id"]
    
    res = @jor.test.find({"description" => "very long description that we might not want to index"}, :reversed => true)
    assert_equal 1, res.size
    assert_equal 42, res.first["_id"]
    
  end
  
end
