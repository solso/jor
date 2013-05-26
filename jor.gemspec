# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'jor/version'

Gem::Specification.new do |gem|
  gem.name          = "jor"

  gem.authors       = ["Josep M. Pujol"]
  gem.email         = 'josep@3scale.net'

  gem.description   = %q{JSON on top of Redis}
  gem.summary       = %q{Storage engine for JSON documents using Redis. It allows fast find operations (index) by any field of the JSON document (ala MongoDB)}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.version       = JOR::VERSION

  gem.add_dependency 'hiredis',       	'0.4.5'
  gem.add_dependency 'redis',         	'3.0.3'
  gem.add_dependency 'rake'
  gem.add_dependency 'rack',		'1.5.2'	
  gem.add_dependency 'rack-test'
  gem.add_dependency 'mongrel'
end

