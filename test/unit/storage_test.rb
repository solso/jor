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
    
    assert_equal doc1.to_json, @jor.find({"_id" => 1}).to_json
    ## use diff when the same order is not guaranted, safer
    assert_equal [], diff(doc2, @jor.find({"_id" => 2}))
    assert_equal [], diff(doc3, @jor.find({"_id" => 3}))
  end
  
  def test_search_by_string_field
    
    docs = []
    10.times do |i|
      docs << @jor.insert({"_id" => i, "name" => "foo_#{i}"})
    end

    doc = @jor.find({"name" => "foo_5"})
    assert_equal docs[5].to_json, doc.to_json
    
    doc = @jor.find({"name" => "foo_7"})
    assert_equal docs[7].to_json, doc.to_json
    
  end
  
end
