# -*- encoding: utf-8 -*-
$:.unshift File.expand_path("../lib", __FILE__)

require 'skydb/version'

Gem::Specification.new do |s|
  s.name        = "skydb"
  s.version     = SkyDB::VERSION
  s.authors     = ["Ben Johnson"]
  s.email       = ["benbjohnson@yahoo.com"]
  s.homepage    = "http://github.com/skydb/sky.rb"
  s.summary     = "A Ruby client for the Sky database"

  s.add_development_dependency('rake', '~> 10.0.4')
  s.add_development_dependency('minitest', '~> 4.7.0')
  s.add_development_dependency('mocha', '~> 0.13.3')
  s.add_development_dependency('unindentable', '~> 0.1.0')
  s.add_development_dependency('simplecov', '~> 0.7.1')
  s.add_development_dependency('webmock', '~> 1.11.0')
  s.add_development_dependency('em-http-request', '~> 1.0.3')
  s.add_development_dependency('em-synchrony', '~> 1.0.3')

  s.test_files   = Dir.glob("test/**/*")
  s.files        = Dir.glob("lib/**/*") + %w(README.md)
  s.require_path = 'lib'
end
