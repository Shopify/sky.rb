# encoding: binary
require 'test_helper'
require 'em-synchrony'
require 'em-synchrony/em-http'

class TestSynchrony < MiniTest::Unit::TestCase
  ##############################################################################
  #
  # Setup / Teardown
  #
  ##############################################################################

  def setup
    # SkyDB.debug = true
    WebMock.disable!
    @client = SkyDB::Client.new(:connection => :synchrony)
    begin; SkyDB::Client.new().delete_table(:name => 'sky-rb-integration'); rescue; end
  end

  def teardown
    begin; SkyDB::Client.new().delete_table(:name => 'sky-rb-integration'); rescue; end
  end

  
  ##############################################################################
  #
  # Tests
  #
  ##############################################################################

  def test_synchrony_add_event
    EM.synchrony do
      table = @client.create_table(:name => 'sky-rb-integration')
      table.create_property(:name => 'action', :transient => true, :data_type => 'factor')
      table.add_event("obj0", :timestamp => DateTime.iso8601('2000-01-01T00:00:00Z'), :data => {'action' => "A0"})
      EM.stop()
    end
  end
end
