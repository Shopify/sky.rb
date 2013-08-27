# encoding: binary
require 'test_helper'

class TestClient < MiniTest::Unit::TestCase
  ##############################################################################
  #
  # Setup / Teardown
  #
  ##############################################################################

  def setup
    @client = SkyDB::Client.new()
    @table = SkyDB::Table.new(:name => 'foo', :client => @client)
  end

  
  ######################################
  # Table API
  ######################################

  def test_get_tables
    stub_request(:get, "http://localhost:8585/tables")
      .to_return(:status => 200, :body => '[{"name":"foo"},{"name":"bar"}]')
    tables = @client.get_tables()
    assert_equal(2, tables.length)
    assert_equal("foo", tables[0].name)
    assert_equal("bar", tables[1].name)
  end

  def test_get_table
    stub_request(:get, "http://localhost:8585/tables/foo")
      .to_return(:status => 200, :body => '{"name":"foo"}')
    table = @client.get_table("foo")
    assert_equal("foo", table.name)
  end

  def test_create_table
    stub_request(:post, "http://localhost:8585/tables")
      .with(:body => '{"name":"foo"}')
      .to_return(:status => 200, :body => '{"name":"foo"}')

    table = @client.create_table(SkyDB::Table.new(:name => 'foo'))
    assert_equal("foo", table.name)
  end

  def test_delete_table
    stub_request(:delete, "http://localhost:8585/tables/foo").to_return(:status => 200)
    @client.delete_table(SkyDB::Table.new(:name => 'foo'))
  end


  ######################################
  # Property API
  ######################################

  def test_get_properties
    stub_request(:get, "http://localhost:8585/tables/foo/properties")
      .to_return(:status => 200, :body => '[{"id":-1,"name":"action","transient":true,"dataType":"string"},{"id":1,"name":"first_name","transient":false,"dataType":"integer"}]')
    properties = @table.get_properties()
    assert_equal(2, properties.length)
    assert_equal("action", properties[0].name)
    assert_equal("first_name", properties[1].name)
  end

  def test_get_property
    stub_request(:get, "http://localhost:8585/tables/foo/properties/action")
      .to_return(:status => 200, :body => '{"id":-1,"name":"action","transient":true,"dataType":"string"}')
    property = @table.get_property("action")
    assert_equal("action", property.name)
  end

  def test_create_property
    stub_request(:post, "http://localhost:8585/tables/foo/properties")
      .with(:body => '{"name":"action","transient":true,"dataType":"string"}')
      .to_return(:status => 200, :body => '{"id":-1,"name":"action","transient":true,"dataType":"string"}')
    property = @table.create_property(SkyDB::Property.new(:name => 'action', :transient => true, :data_type => 'string'))
    assert_equal("action", property.name)
    assert_equal(true, property.transient)
    assert_equal("string", property.data_type)
  end

  def test_update_property
    stub_request(:patch, "http://localhost:8585/tables/foo/properties/action2")
      .with(:body => '{"name":"action2","transient":true,"dataType":"string","id":-1}')
      .to_return(:status => 200, :body => '{"id":-1,"name":"action2","transient":true,"dataType":"string"}')
    property = @table.update_property(SkyDB::Property.new(:id => -1, :name => 'action2', :transient => true, :data_type => 'string'))
    assert_equal("action2", property.name)
  end

  def test_delete_property
    stub_request(:delete, "http://localhost:8585/tables/foo/properties/action")
      .to_return(:status => 200)
    @table.delete_property(SkyDB::Property.new(:name => 'action'))
  end


  ######################################
  # Object API
  ######################################

  def test_merge_objects
    req = stub_request(:post, "http://localhost:8585/tables/foo/objects/a/merge")
      .with(:body => '{"id":"b"}')
      .to_return(:status => 200)
    @table.merge_objects("a", "b")
    assert_requested(req)
  end


  ######################################
  # Event API
  ######################################

  def test_get_events
    stub_request(:get, "http://localhost:8585/tables/foo/objects/xxx/events")
      .to_return(:status => 200, :body => '[{"timestamp":"1970-01-01T00:00:00Z","data":{"action":"/home"}},{"timestamp":"1970-01-01T00:00:00.5Z","data":{"action":"/pricing"}}]')
    events = @table.get_events("xxx")
    assert_equal(2, events.length)
    assert_equal("1970-01-01T00:00:00.000000Z", events[0].formatted_timestamp)
    assert_equal({'action' => '/home'}, events[0].data)
    assert_equal("1970-01-01T00:00:00.500000Z", events[1].formatted_timestamp)
    assert_equal({'action' => '/pricing'}, events[1].data)
  end

  def test_get_event
    stub_request(:get, "http://localhost:8585/tables/foo/objects/xxx/events/1970-01-01T00:00:00.000000Z")
      .to_return(:status => 200, :body => '{"timestamp":"1970-01-01T00:00:00.000000Z","data":{"action":"/home"}}')
    event = @table.get_event("xxx", DateTime.iso8601('1970-01-01T00:00:00Z'))
    assert_equal("1970-01-01T00:00:00.000000Z", event.formatted_timestamp)
    assert_equal({'action' => '/home'}, event.data)
  end

  def test_replace_event
    stub_request(:put, "http://localhost:8585/tables/foo/objects/xxx/events/1970-01-01T00:00:00.000000Z")
      .with(:body => '{"timestamp":"1970-01-01T00:00:00.000000Z","data":{"action":"/home"}}')
      .to_return(:status => 200, :body => '{"timestamp":"1970-01-01T00:00:00.000000Z","data":{"action":"/home"}}')
    event = SkyDB::Event.new(:timestamp => DateTime.iso8601('1970-01-01T00:00:00Z'), :data => {'action' => '/home'})
    event = @table.add_event("xxx", event, :method => :replace)
    assert_equal("1970-01-01T00:00:00.000000Z", event.formatted_timestamp)
    assert_equal({'action' => '/home'}, event.data)
  end

  def test_merge_event
    stub_request(:patch, "http://localhost:8585/tables/foo/objects/xxx/events/1970-01-01T00:00:00.000000Z")
      .with(:body => '{"timestamp":"1970-01-01T00:00:00.000000Z","data":{"action":"/home"}}')
      .to_return(:status => 200, :body => '{"timestamp":"1970-01-01T00:00:00.000000Z","data":{"action":"/home","first_name":"bob"}}')
    event = SkyDB::Event.new(:timestamp => DateTime.iso8601('1970-01-01T00:00:00Z'), :data => {'action' => '/home'})
    event = @table.add_event("xxx", event, :method => :merge)
    assert_equal("1970-01-01T00:00:00.000000Z", event.formatted_timestamp)
    assert_equal({'action' => '/home', 'first_name' => 'bob'}, event.data)
  end

  def test_delete_event
    stub_request(:delete, "http://localhost:8585/tables/foo/objects/xxx/events/1970-01-01T00:00:00.000000Z")
      .to_return(:status => 200)
    @table.delete_event("xxx", SkyDB::Event.new(:timestamp => DateTime.iso8601('1970-01-01T00:00:00Z')))
  end


  ######################################
  # Query API
  ######################################

  def test_query
    stub_request(:post, "http://localhost:8585/tables/foo/query")
      .with(:body => '{"statements":[{"type":"selection","fields":[{"name":"count","expression":"count()"}]}]}')
      .to_return(:status => 200, :body => '{"count":5}'+"\n")
    results = @table.query([{:type => 'selection', :fields => [{:name => 'count', :expression => 'count()'}]}])
    assert_equal({'count' => 5}, results)
  end


  ######################################
  # Utility API
  ######################################

  def test_successful_ping
    stub_request(:get, "http://localhost:8585/ping")
      .to_return(:status => 200, :body => '{"message":"ok"}')
    assert(@client.ping())
  end

  def test_unsuccessful_ping
    stub_request(:get, "http://localhost:8585/ping").to_timeout
    refute(@client.ping())
  end
end
