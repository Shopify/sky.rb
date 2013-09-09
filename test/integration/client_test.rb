# encoding: binary
require 'test_helper'

class TestClient < MiniTest::Unit::TestCase
  ##############################################################################
  #
  # Setup / Teardown
  #
  ##############################################################################

  def setup
    # SkyDB.debug = true
    WebMock.disable!
    @client = SkyDB::Client.new()
    begin; @client.delete_table(:name => 'sky-rb-integration'); rescue; end
  end

  def teardown
    begin; @client.delete_table(:name => 'sky-rb-integration'); rescue; end
  end

  
  ##############################################################################
  #
  # Tests
  #
  ##############################################################################

  def test_event_management
    table = @client.create_table(:name => 'sky-rb-integration')
    table.create_property(:name => 'action', :transient => true, :data_type => 'factor')
    table.create_property(:name => 'age', :data_type => 'integer')

    table.add_event("obj0", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A0"})
    table.add_event("obj0", :timestamp => DateTime.iso8601('2013-01-01T00:00:01Z'), :data => {'action' => "A1"})
    table.add_event("obj0", :timestamp => DateTime.iso8601('2013-01-01T00:00:01Z'), :data => {'age' => 12})
    table.add_event("obj0", :timestamp => DateTime.iso8601('2013-01-01T00:00:02Z'), :data => {'action' => "A2"})
    table.delete_event("obj0", :timestamp => DateTime.iso8601('2013-01-01T00:00:02Z'))

    table.add_event("obj1", :timestamp => DateTime.iso8601('2013-01-01T01:00:00Z'), :data => {'action' => "A1"})
    table.add_event("obj1", {:timestamp => DateTime.iso8601('2013-01-01T01:00:00Z'), :data => {'age' => 50}}, :method => :replace)

    assert_equal({"timestamp"=>"2013-01-01T01:00:00.000000Z", "data"=>{"age" => 50}}, table.get_event("obj1", DateTime.parse("2013-01-01T01:00:00Z")).to_hash)
    assert_equal([
      {"timestamp"=>"2013-01-01T00:00:00.000000Z", "data"=>{"action"=>"A0"}},
      {"timestamp"=>"2013-01-01T00:00:01.000000Z", "data"=>{"action"=>"A1", "age" => 12}}
      ], table.get_events("obj0").map{|e| e.to_hash})

    @client.delete_table(table)
  end

  def test_object_merge
    table = @client.create_table(:name => 'sky-rb-integration')
    table.create_property(:name => 'action', :transient => true, :data_type => 'factor')

    table.add_event("a", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A0"})
    table.add_event("a", :timestamp => DateTime.iso8601('2013-01-03T00:00:00Z'), :data => {'action' => "A2"})
    table.add_event("b", :timestamp => DateTime.iso8601('2013-01-02T00:00:00Z'), :data => {'action' => "A1"})
    table.merge_objects("a", "b")

    assert_equal([
      {"timestamp"=>"2013-01-01T00:00:00.000000Z", "data"=>{"action"=>"A0"}},
      {"timestamp"=>"2013-01-02T00:00:00.000000Z", "data"=>{"action"=>"A1"}},
      {"timestamp"=>"2013-01-03T00:00:00.000000Z", "data"=>{"action"=>"A2"}}],
      table.get_events("a").map{|e| e.to_hash}
    )
    assert_equal([], table.get_events("b").map{|e| e.to_hash})

    @client.delete_table(table)
  end

  def test_simple_count_query
    table = @client.create_table(:name => 'sky-rb-integration')
    table.create_property(:name => 'action', :transient => true, :data_type => 'factor')
    table.add_event("count0", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A0"})
    table.add_event("count0", :timestamp => DateTime.iso8601('2013-01-01T00:00:01Z'), :data => {'action' => "A1"})
    table.add_event("count0", :timestamp => DateTime.iso8601('2013-01-01T00:00:02Z'), :data => {'action' => "A2"})
    table.add_event("count1", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A1"})
    table.add_event("count1", :timestamp => DateTime.iso8601('2013-01-01T00:00:05Z'), :data => {'action' => "A2"})
    results = table.query({steps:[{:type => 'selection', :fields => [:name => 'count', :expression => 'count()']}]})
    stats = table.stats()
    @client.delete_table(table)
    assert_equal({'count' => 5}, results)
    assert_equal({'count' => 5}, stats)
  end

  def test_funnel_query
    table = @client.create_table(:name => 'sky-rb-integration')
    table.create_property(:name => 'action', :transient => true, :data_type => 'factor')
    table.add_event("fun0", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A0"})
    table.add_event("fun0", :timestamp => DateTime.iso8601('2013-01-01T00:00:01Z'), :data => {'action' => "A1"})
    table.add_event("fun0", :timestamp => DateTime.iso8601('2013-01-01T00:00:02Z'), :data => {'action' => "A2"})
    table.add_event("fun1", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A0"})
    table.add_event("fun1", :timestamp => DateTime.iso8601('2013-01-01T00:00:05Z'), :data => {'action' => "A1"})
    table.add_event("fun1", :timestamp => DateTime.iso8601('2013-01-01T00:00:10Z'), :data => {'action' => "A3"})
    results = table.query({
      statements:[
        {:type => 'condition', :expression => 'action == "A0"', :statements => [
          {:type => 'condition', :expression => 'action == "A1"', :within => [1,1], :statements => [
            {:type => 'condition', :expression => 'true', :within => [1,1], :statements => [
              {:type => 'selection', :dimensions => ['action'], :fields => [:name => 'count', :expression => 'count()']}
            ]}
          ]}
        ]}
      ]
    })
    @client.delete_table(table)
    assert_equal({"action"=>{"A2"=>{"count"=>1}, "A3"=>{"count"=>1}}}, results)
  end

  def test_skyql_query
    table = @client.create_table(:name => 'sky-rb-integration')
    table.create_property(:name => 'action', :transient => true, :data_type => 'factor')
    table.add_event("count0", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A0"})
    table.add_event("count0", :timestamp => DateTime.iso8601('2013-01-01T00:00:01Z'), :data => {'action' => "A1"})
    table.add_event("count0", :timestamp => DateTime.iso8601('2013-01-01T00:00:02Z'), :data => {'action' => "A2"})
    table.add_event("count1", :timestamp => DateTime.iso8601('2013-01-01T00:00:00Z'), :data => {'action' => "A1"})
    table.add_event("count1", :timestamp => DateTime.iso8601('2013-01-01T00:00:05Z'), :data => {'action' => "A2"})
    results = table.query("SELECT count() AS myCount GROUP BY action")
    @client.delete_table(table)

    assert_equal({"action"=>{"A0"=>{"myCount"=>1}, "A1"=>{"myCount"=>2}, "A2"=>{"myCount"=>2}}}, results)
  end

  def test_querying_string_values_with_quotes
    table = @client.create_table(:name => 'sky-rb-integration')
    table.create_property(:name => 'action', :transient => true, :data_type => 'string')
    values = ["", "'", '"', '"\'']
    values.each{ | string | table.add_event("000", :timestamp => DateTime.now, :data => {'action' => string}) }
    #confirm that the values made it into the database unmodified
    result = table.query({statements: 'SELECT count() AS count GROUP BY action'})['action']
    assert_equal result.keys.sort, values.sort
    result.each { |k,v| assert_equal({'count' => 1}, v)}
    [ "''",
      '""',
      "'\"'",
      '"\'"',
      '"\\"\'"',
      "'\"\\\''",
    ].each_with_index do | expression, index |
      query = "WHEN action == #{expression} THEN SELECT count() AS count END"
      result = table.query({statements: query})
      assert_equal({'count' => 1}, result, "Failed expression #{index}: action == #{expression}")
    end
  ensure
    @client.delete_table(table) if table
  end  
end
