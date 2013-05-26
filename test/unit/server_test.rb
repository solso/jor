require File.expand_path(File.dirname(__FILE__) + '/../test_helper')

class ServerTest < Test::Unit::TestCase
  include Rack::Test::Methods
  
  def app
    JOR::Server.new
  end
  
  def setup
    redis = Redis.new(:db => 9, :driver => :hiredis)
    @jor = JOR::Storage.new(redis)
    list = @jor.redis.keys("*")
    raise "Cannot run the tests safely!! The test DB (:db => 9) is not empty, and the test might flush the data. Stopping." if list.size>0
    @jor.create_collection("test")
  end
  
  def teardown
    @jor.redis.flushdb()
  end
  
  def test_calling_methods
    
    get '/last_id' 
    assert_equal 422, last_response.status
    assert_equal "Collection \"last_id\" does not exist", JSON::parse(last_response.body)["error"]
    
    get '/test.last_id'
    assert_equal 200, last_response.status
    assert_equal 1, JSON::parse(last_response.body)["value"]
    
    post '/test.last_id'
    assert_equal 200, last_response.status
    assert_equal 1, JSON::parse(last_response.body)["value"]
    
    doc1 = create_sample_doc_restaurant({"_id" => 1})

    get '/test.insert', "args[]=#{doc1.to_json}"
    assert_equal 200, last_response.status

    doc2 = create_sample_doc_restaurant({"_id" => 2})
    get '/test.insert', "args[]=#{doc2.to_json}"
    assert_equal 200, last_response.status
    
    docs = [create_sample_doc_restaurant({"_id" => 3}), create_sample_doc_restaurant({"_id" => 4})]
    get '/test.insert', "args[]=#{docs.to_json}"
    assert_equal 200, last_response.status

    get '/test.find', "args[]=#{{"_id" => 4}.to_json}"
    assert_equal 200, last_response.status
    results = JSON::parse(last_response.body)
    assert_equal 1, results.size
    
    get '/test.find', "args[]=#{{}.to_json}"
    assert_equal 200, last_response.status
    results = JSON::parse(last_response.body)
    assert_equal 4, results.size
    4.times do |i|
      assert_equal i+1, results[i]["_id"]
    end

    get '/test.find', "args[]=#{{}.to_json}&args[]=#{{"reversed" => true}.to_json}"
    assert_equal 200, last_response.status
    results = JSON::parse(last_response.body)
    assert_equal 4, results.size
    4.times do |i|
      assert_equal 4-i, results[i]["_id"]
    end
    
    post '/test.find', "args[]=#{{}.to_json}&args[]=#{{"reversed" => true}.to_json}"
    assert_equal 200, last_response.status
    results = JSON::parse(last_response.body)
    assert_equal 4, results.size
    4.times do |i|
      assert_equal 4-i, results[i]["_id"]
    end
    
    get '/test.find', "args[]=#{{"fake" => "super_fake"}.to_json}"
    assert_equal 200, last_response.status
    results = JSON::parse(last_response.body)
    assert_equal 0, results.size
    
  end
  
  
end