require File.expand_path(File.dirname(__FILE__) + '/../test_helper')

class StorageTest < Test::Unit::TestCase

  def setup 
    redis = Redis.new(:db => 9)
    @jor = ::JOR::Storage.new(redis)
    list = @jor.redis.keys("*")
    raise "Cannot run the tests safely!! The test DB (:db => 9) is not empty, and the test might flush the data. Stopping." if list.size>0
  end
  
  def teardown
    @jor.redis.flushdb()
  end
  
  
  def test_basic_save_and_find_path
    doc1 = create_sample_doc_restaurant({"_id" => 1})
    @jor.insert(doc1)
    
    doc2 = create_sample_doc_restaurant({"_id" => 2})
    @jor.insert(doc2)
    
    doc3 = create_sample_doc_restaurant({"_id" => 3})
    @jor.insert(doc3)
    
    assert_equal doc1.to_json, @jor.find({"_id" => 1}).first.to_json
    ## use diff when the same order is not guaranted, safer
    assert_equal [], diff(doc2, @jor.find({"_id" => 2}).first)
    assert_equal [], diff(doc3, @jor.find({"_id" => 3}).first)
  end
  
  def WIP_test_search_by_string_field
    
    docs = []
    10.times do |i|
      docs << @jor.insert({"_id" => i, "name" => "foo_#{i}"})
    end

    doc = @jor.find({"name" => "foo_5"}).first
    assert_equal docs[5].to_json, doc.to_json
    
    doc = @jor.find({"name" => "foo_7"}).first
    assert_equal docs[7].to_json, doc.to_json
    
  end
  
  def test_search_by_numeric_field
    
    docs = []
    ## years from 2000 to 2009
    10.times do |i|
      docs << @jor.insert({"_id" => i, "name" => "foo_#{i}", "year" => 2000+i})
    end
    
    doc = @jor.find({"year" => 2005}).first
    assert_equal docs[5].to_json, doc.to_json
    
    doc = @jor.find({"year" => { "$lt" => 2005 }})
    assert_equal 5, doc.size
    assert_equal docs[0].to_json, doc.first.to_json
    assert_equal docs[4].to_json, doc.last.to_json
    
    doc = @jor.find({"year" => { "$lte" => 2005 }})
    assert_equal 6, doc.size
    assert_equal docs[0].to_json, doc.first.to_json
    assert_equal docs[5].to_json, doc.last.to_json
    
    doc = @jor.find({"year" => { "$gt" => 2007 }})
    assert_equal 2, doc.size
    assert_equal docs[8].to_json, doc.first.to_json
    assert_equal docs[9].to_json, doc.last.to_json
    
    doc = @jor.find({"year" => { "$gte" => 2007 }})
    assert_equal 3, doc.size
    assert_equal docs[7].to_json, doc.first.to_json
    assert_equal docs[9].to_json, doc.last.to_json
    
    doc = @jor.find({"year" => { "$gte" => 2003, "$lt" => 2005 }})
    assert_equal 2, doc.size
    assert_equal docs[3].to_json, doc.first.to_json
    assert_equal docs[4].to_json, doc.last.to_json

    doc = @jor.find({"year" => { "$gt" => 2003, "$lt" => 9999 }})
    assert_equal 6, doc.size
    assert_equal docs[4].to_json, doc.first.to_json
    assert_equal docs[9].to_json, doc.last.to_json
     
  end
  
  def test_search_by_string_and_numeric_field_multiple
    
    sample_docs = []
    ## years from 2000 to 2009
    10.times do |i|
      sample_docs << @jor.insert({"_id" => i, "year" => 2000+i, "desc" => "bar", "nested" => {"name" => "foo_#{i}", "quantity" => i.to_f}})
    end

    docs = @jor.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "nested" => {"quantity" => {"$lt" => 1.0}}})
    assert_equal 0, docs.size
    assert_equal [], docs
    
    docs = @jor.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "nested" => {"quantity" => {"$lt" => 4.0}}})
    assert_equal 2, docs.size
    assert_equal sample_docs[2].to_json, docs.first.to_json
    assert_equal sample_docs[3].to_json, docs.last.to_json
    
    docs = @jor.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "nested" => {"name" => "foo_#{4}", "quantity" => {"$lte" => 4.0}}})
    assert_equal 1, docs.size
    assert_equal sample_docs[4].to_json, docs.first.to_json
    
    docs = @jor.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "desc" => "bar", "nested" => {"name" => "foo_#{4}", "quantity" => {"$lte" => 4.0}}})
    assert_equal 1, docs.size
    assert_equal sample_docs[4].to_json, docs.first.to_json
        
    docs = @jor.find({"year" => { "$gt" => 2001, "$lt" => 2009 }, "desc" => "NOT_bar", "nested" => {"name" => "foo_#{4}", "quantity" => {"$lte" => 4.0}}})
    assert_equal 0, docs.size
        
  end
  
end
