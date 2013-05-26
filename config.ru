

require File.dirname(__FILE__) + "/lib/jor.rb"

Rack::Handler::Mongrel.run JOR::Server.new 


