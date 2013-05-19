require File.expand_path(File.dirname(__FILE__) + '/../test_helper')

class StorageTest < Test::Unit::TestCase

  def setup 
    redis = Redis.new(:db => 9)
    @jor = JOR::Storage.new(redis)
    list = @jor.redis.keys("*")
    raise "Cannot run the tests safely!! The test DB (:db => 9) is not empty, and the test might flush the data. Stopping." if list.size>0
  end
  
  def teardown
    @jor.redis.flushdb()
  end
  
  def test_create_collection
    @jor.create_collection("coll_foo")
    @jor.create_collection("coll_bar")
    assert_equal ["coll_foo", "coll_bar"].sort, @jor.list_collections.sort
    
    @jor.create_collection("coll_zoe")
    assert_equal ["coll_foo", "coll_bar", "coll_zoe"].sort, @jor.list_collections.sort
    
    assert_raise JOR::CollectionAlreadyExists do
      @jor.create_collection("coll_zoe")
    end
    assert_equal ["coll_foo", "coll_bar", "coll_zoe"].sort, @jor.list_collections.sort
    
    assert_raise JOR::CollectionNotValid do
      @jor.create_collection("collections")
    end
    assert_equal ["coll_foo", "coll_bar", "coll_zoe"].sort, @jor.list_collections.sort
    
  end
  
  def test_destroy_collection
    @jor.create_collection("coll_1")
    @jor.create_collection("coll_2")
    @jor.create_collection("coll_3")
    assert_equal ["coll_1", "coll_2", "coll_3"].sort, @jor.list_collections.sort
    
    assert_raise JOR::CollectionDoesNotExist do
      @jor.destroy_collection("foo")
    end
    assert_equal ["coll_1", "coll_2", "coll_3"].sort, @jor.list_collections.sort
    
    @jor.destroy_collection("coll_1")
    assert_equal ["coll_2", "coll_3"].sort, @jor.list_collections.sort

    @jor.destroy_all()
    assert_equal [].sort, @jor.list_collections.sort  
  end
  
  def test_collection_has_not_been_created_or_removed
    
    assert_raise JOR::CollectionDoesNotExist do
      @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 1}))
    end
    
    @jor.create_collection("restaurant")
    @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 1}))
    @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 2}))
    @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 3}))
    assert_equal 3, @jor.restaurant.count()
    
    @jor.destroy_collection("restaurant")
    
    assert_raise JOR::CollectionDoesNotExist do
      @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 1}))
    end
    
  end
  
  def test_switching_between_collections
    
    @jor.create_collection("restaurant")
    @jor.create_collection("cs")
    
    10.times do |i|
      @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => i})) 
    end
    
    assert_equal 10, @jor.restaurant.count()
    assert_equal 0, @jor.cs.count()
    
    100.times do |i|
      @jor.cs.insert(create_sample_doc_cs({"_id" => i})) 
    end
    assert_equal 10, @jor.restaurant.count()
    assert_equal 100, @jor.cs.count()
    
    @jor.destroy_collection("restaurant")
    assert_raise JOR::CollectionDoesNotExist do
      @jor.restaurant.count()
    end
    assert_equal 100, @jor.cs.count()
    
  end      
end