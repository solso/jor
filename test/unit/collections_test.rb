require File.expand_path(File.dirname(__FILE__) + '/../test_helper')

class CollectionsTest < Test::Unit::TestCase

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
    @jor.collections.create("coll_foo")
    @jor.collections.create("coll_bar")
    assert_equal ["coll_foo", "coll_bar"].sort, @jor.collections.list.sort
    
    @jor.collections.create("coll_zoe")
    assert_equal ["coll_foo", "coll_bar", "coll_zoe"].sort, @jor.collections.list.sort
    
    assert_raise JOR::CollectionAlreadyExists do
      @jor.collections.create("coll_zoe")
    end
    assert_equal ["coll_foo", "coll_bar", "coll_zoe"].sort, @jor.collections.list.sort
    
    assert_raise JOR::CollectionNotValid do
      @jor.collections.create("collections")
    end
    assert_equal ["coll_foo", "coll_bar", "coll_zoe"].sort, @jor.collections.list.sort
    
  end
  
  def test_destroy_collection
    @jor.collections.create("coll_1")
    @jor.collections.create("coll_2")
    @jor.collections.create("coll_3")
    assert_equal ["coll_1", "coll_2", "coll_3"].sort, @jor.collections.list.sort
    
    assert_raise JOR::CollectionDoesNotExist do
      @jor.collections.destroy("foo")
    end
    assert_equal ["coll_1", "coll_2", "coll_3"].sort, @jor.collections.list.sort
    
    @jor.collections.destroy("coll_1")
    assert_equal ["coll_2", "coll_3"].sort, @jor.collections.list.sort

    @jor.collections.destroy_all()
    assert_equal [].sort, @jor.collections.list.sort  
  end
  
  def test_collection_has_not_been_created_or_removed
    
    assert_raise JOR::CollectionDoesNotExist do
      @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 1}))
    end
    
    @jor.collections.create("restaurant")
    @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 1}))
    @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 2}))
    @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 3}))
    assert_equal 3, @jor.restaurant.count()
    
    @jor.collections.destroy("restaurant")
    
    assert_raise JOR::CollectionDoesNotExist do
      @jor.restaurant.insert(create_sample_doc_restaurant({"_id" => 1}))
    end
    
  end
  
  def test_switching_between_collections
    
    @jor.collections.create("restaurant")
    @jor.collections.create("cs")
    
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
    
    @jor.collections.destroy("restaurant")
    assert_raise JOR::CollectionDoesNotExist do
      @jor.restaurant.count()
    end
    assert_equal 100, @jor.cs.count()
    
  end      
end