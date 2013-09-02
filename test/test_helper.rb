require 'simplecov'
SimpleCov.start
require 'bundler/setup'
require 'minitest/autorun'
require 'mocha/setup'
require 'webmock/minitest'
require 'unindentable'
require 'em-http'
require 'skydb'

class MiniTest::Unit::TestCase
  def fixture(path)
    File.join(File.expand_path('../fixtures', __FILE__), path)
  end
end
