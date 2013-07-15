class SkyDB
  class Client
    ##########################################################################
    #
    # Errors
    #
    ##########################################################################

    class ServerError < SkyError
      attr_accessor :status
    end

    
    ##########################################################################
    #
    # Constants
    #
    ##########################################################################

    # The default type of connection to use if one is not specified.
    DEFAULT_CONNECTION_TYPE = :ruby

    # The default host to connect to if one is not specified.
    DEFAULT_HOST = 'localhost'

    # The default port to connect to if one is not specified.
    DEFAULT_PORT = 8585

    #use http by default
    USE_SSL = false

    ##########################################################################
    #
    # Constructor
    #
    ##########################################################################

    # Initializes the client.
    def initialize(options={})
      self.connection = options[:connection] || DEFAULT_CONNECTION_TYPE
      self.host = options[:host] || DEFAULT_HOST
      self.port = options[:port] || DEFAULT_PORT
      self.ssl = options[:ssl] || USE_SSL
    end


    ##########################################################################
    #
    # Attributes
    #
    ##########################################################################

    ####################################
    # Connection Properties
    ####################################

    # The name of the host to conect to.
    attr_accessor :host

    # The port on the host to connect to.
    attr_accessor :port

    # Enable/Disable HTTPS
    attr_accessor :ssl


    ####################################
    # Connection
    ####################################

    # The connection to be used to transfer data to Sky. If the connection is
    # set to a symbol then a connection of that type will be created. The
    # :ruby and :synchrony connection types are currently available.
    attr_accessor :connection

    def connection=(value)
      if value.is_a?(Symbol)
        case value
        when :ruby
          value = SkyDB::Connection::Ruby.new()
        when :synchrony
          value = SkyDB::Connection::Synchrony.new()
        else
          raise SkyError.new("Invalid connection type: #{value}")
        end
      end
      
      @connection = value
    end


    ##########################################################################
    #
    # Methods
    #
    ##########################################################################
    
    ####################################
    # Table API
    ####################################

    # Retrieves a list of tables on the server.
    def get_tables(options={})
      data = send(:get, "/tables")
      tables = data.map {|i| Table.new(:client => self).from_hash(i)}
      return tables
    end

    # Retrieves a single table from the server.
    def get_table(name, options={})
      raise ArgumentError.new("Table name required") if name.nil?
      data = send(:get, "/tables/#{name}")
      table = Table.new(:client => self).from_hash(data)
      return table
    end

    # Creates a table on the server.
    #
    # @param [Table] table  the table to create.
    def create_table(table, options={})
      raise ArgumentError.new("Table required") if table.nil?
      table = Table.new(table) if table.is_a?(Hash)
      table.client = self
      data = send(:post, "/tables", table.to_hash)
      return table.from_hash(data)
    end

    # Deletes a table on the server.
    #
    # @param [Table] table  the table to delete.
    def delete_table(table, options={})
      raise ArgumentError.new("Table required") if table.nil?
      table = Table.new(table) if table.is_a?(Hash)
      table.client = self
      send(:delete, "/tables/#{table.name}")
      return nil
    end


    ####################################
    # Property API
    ####################################

    # Retrieves a list of all properties on a table.
    #
    # @return [Array]  the list of properties on the table.
    def get_properties(table, options={})
      raise ArgumentError.new("Table required") if table.nil?
      properties = send(:get, "/tables/#{table.name}/properties")
      properties.map!{|p| Property.new().from_hash(p)}
      return properties
    end

    # Retrieves a single property by name.
    #
    # @param [Table] table  The table to retrieve from.
    # @param [String] name  The name of the property to retrieve.
    #
    # @return [Array]  the list of properties on the table.
    def get_property(table, name, options={})
      raise ArgumentError.new("Table required") if table.nil?
      data = send(:get, "/tables/#{table.name}/properties/#{name}")
      return Property.new().from_hash(data)
    end

    # Creates a property on a table.
    #
    # @param [Property] property  the property to create.
    def create_property(table, property, options={})
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Property required") if property.nil?
      property = Property.new(property) if property.is_a?(Hash)
      data = send(:post, "/tables/#{table.name}/properties", property.to_hash)
      return property.from_hash(data)
    end

    # Updates a property on a table.
    #
    # @param [Property] property  the property to update.
    def update_property(table, property, options={})
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Property required") if property.nil?
      raise ArgumentError.new("Property name required") if property.name.to_s == ''
      property = Property.new(property) if property.is_a?(Hash)
      data = send(:patch, "/tables/#{table.name}/properties/#{property.name}", property.to_hash)
      return property.from_hash(data)
    end

    # Deletes a property on a table.
    #
    # @param [Property] property  the property to delete.
    def delete_property(table, property, options={})
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Property required") if property.nil?
      raise ArgumentError.new("Property name required") if property.name.to_s == ''
      property = Property.new(property) if property.is_a?(Hash)
      send(:delete, "/tables/#{table.name}/properties/#{property.name}")
      return nil
    end


    ####################################
    # Event API
    ####################################

    # Merges events from one object into another. The source object will be
    # deleted after the merge is complete.
    #
    # @param [Table] table  the table that the objects belong to.
    # @param [String] dest_object_id  the id of the object to merge into.
    # @param [String] src_object_id  the id of the object to merge from.
    def merge_objects(table, dest_object_id, src_object_id, options={})
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Destination object identifier required") if dest_object_id.nil?
      raise ArgumentError.new("Source object identifier required") if src_object_id.nil?
      send(:post, "/tables/#{table.name}/objects/#{dest_object_id}/merge", {'id' => src_object_id.to_s})
      return nil
    end


    ####################################
    # Event API
    ####################################

    # Retrieves all events for a given object.
    #
    # @return [Array]  the list of events on the table.
    def get_events(table, object_id, options={})
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Object identifier required") if object_id.nil?
      events = send(:get, "/tables/#{table.name}/objects/#{object_id}/events")
      events.map!{|e| Event.new().from_hash(e)}
      return events
    end

    # Retrieves the event that occurred at a given point in time for an object.
    #
    # @return [Event]  the event.
    def get_event(table, object_id, timestamp, options={})
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Object identifier required") if object_id.nil?
      raise ArgumentError.new("Timestamp required") if timestamp.nil?
      data = send(:get, "/tables/#{table.name}/objects/#{object_id}/events/#{SkyDB.format_timestamp(timestamp)}")
      return Event.new().from_hash(data)
    end

    # Adds an event to an object.
    #
    # @param [Table] table  the table the object belongs to.
    # @param [String] object_id  the object's identifier.
    # @param [Event] event  the event to add.
    #
    # @return [Event]  the event.
    def add_event(table, object_id, event, options={})
      options = {:method => :merge}.merge(options)
      
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Object identifier required") if object_id.nil?
      raise ArgumentError.new("Event required") if event.nil?
      event = Event.new(event) if event.is_a?(Hash)
      raise ArgumentError.new("Event timestamp required") if event.timestamp.nil?

      # The insertion method is communicated to the server through the HTTP method.
      http_method = case options[:method]
        when :replace then :put
        when :merge then :patch
        else raise ArgumentError.new("Invalid event insertion method: #{options[:method]}")
        end

      # Send the event and parse it when it comes back. It could have changed.
      data = send(http_method, "/tables/#{table.name}/objects/#{object_id}/events/#{SkyDB.format_timestamp(event.timestamp)}", event.to_hash)
      return event.from_hash(data)
    end

    # Deletes an event for an object on a table.
    #
    # @param [Table] table  the table the object belongs to.
    # @param [String] object_id  the object's identifier.
    # @param [Event] event  the event to delete.
    def delete_event(table, object_id, event, options={})
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Object identifier required") if object_id.nil?
      raise ArgumentError.new("Event required") if event.nil?
      event = Event.new(event) if event.is_a?(Hash)
      raise ArgumentError.new("Event timestamp required") if event.timestamp.nil?
      send(:delete, "/tables/#{table.name}/objects/#{object_id}/events/#{SkyDB.format_timestamp(event.timestamp)}")
      return nil
    end


    ####################################
    # Query API
    ####################################

    # Runs a query against a given table.
    #
    # @param [Table] table  The table to query.
    # @param [Hash] q  The query definition to run.
    #
    # @return [Results]  the results of the query.
    def query(table, q)
      raise ArgumentError.new("Table required") if table.nil?
      raise ArgumentError.new("Query definition required") if q.nil?
      q = {:steps => q} if q.is_a?(Array)
      return send(:post, "/tables/#{table.name}/query", q)
    end

    # Retrieves stats such as the event count on a given table.
    #
    # @param [Table] table  The table to retrieve stats for.
    #
    # @return [Results]  the table stats.
    def stats(table)
      raise ArgumentError.new("Table required") if table.nil?
      return send(:get, "/tables/#{table.name}/stats")
    end


    ####################################
    # Utility API
    ####################################

    # Pings the server to determine if it is running.
    #
    # @return [Boolean]  true if the server is running, otherwise false.
    def ping(options={})
      begin
        send(:get, "/ping")
      rescue
        return false
      end
      return true
    end


    ####################################
    # HTTP Utilities
    ####################################
    
    # Executes a RESTful JSON over HTTP POST.
    def send(method, path, data=nil)
      if @connection.nil?
        raise SkyError.new("Connection unavailable")
      end
      
      # Send data over connection and return the results.
      ret = connection.send(
        :host => host,
        :port => port,
        :ssl => ssl,
        :method => method,
        :path => path,
        :data => data
      )
      return ret
    end
  end
end