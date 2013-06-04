class SkyDB
  class Table
    ##########################################################################
    #
    # Constructor
    #
    ##########################################################################

    # Initializes the table.
    def initialize(options={})
      self.client = options[:client]
      self.name = options[:name]
    end
    

    ##########################################################################
    #
    # Attributes
    #
    ##########################################################################

    # The client this table is associated with.
    attr_accessor :client

    # The name of the table.
    attr_accessor :name


    ##########################################################################
    #
    # Methods
    #
    ##########################################################################

    ####################################
    # Property API
    ####################################

    # Retrieves a list of all properties for the table.
    #
    # @return [Array]  the list of properties on the table.
    def get_properties(options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.get_properties(self, options)
    end

    # Retrieves a single property by name from the table.
    #
    # @param [String] name  The name of the property to retrieve.
    #
    # @return [Array]  the list of properties on the table.
    def get_property(name, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.get_property(self, name, options)
    end

    # Creates a property on a table.
    #
    # @param [Property] property  the property to create.
    def create_property(property, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.create_property(self, property, options)
    end

    # Updates a property on a table.
    #
    # @param [Property] property  the property to update.
    def update_property(property, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.update_property(self, property, options)
    end

    # Deletes a property on a table.
    #
    # @param [Property] property  the property to delete.
    def delete_property(property, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.delete_property(self, property, options)
    end


    ####################################
    # Event API
    ####################################

    # Retrieves all events for a given object.
    #
    # @return [Array]  the list of events on the table.
    def get_events(object_id, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.get_events(self, object_id, options)
    end

    # Retrieves the event that occurred at a given point in time for an object.
    #
    # @return [Event]  the event.
    def get_event(object_id, timestamp, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.get_event(self, object_id, timestamp, options)
    end

    # Adds an event to an object.
    #
    # @param [String] object_id  the object's identifier.
    # @param [Event] event  the event to add.
    #
    # @return [Event]  the event.
    def add_event(object_id, event, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.add_event(self, object_id, event, options)
    end

    # Deletes an event for an object on a table.
    #
    # @param [String] object_id  the object's identifier.
    # @param [Event] event  the event to delete.
    def delete_event(object_id, event, options={})
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.delete_event(self, object_id, event, options)
    end


    ####################################
    # Query API
    ####################################

    # Runs a query against a given table.
    #
    # @param [Hash] q  The query definition to run.
    #
    # @return [Results]  the results of the query.
    def query(q)
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.query(self, q)
    end

    # Retrieves stats for a given table.
    #
    # @return [Results]  the table stats.
    def stats()
      raise ArgumentError.new("Table not associated with client") if client.nil?
      return client.stats(self)
    end


    ####################################
    # Encoding
    ####################################

    # Encodes the table into a hash.
    def to_hash(*a)
      {'name' => name}
    end

    # Decodes a hash into a table.
    def from_hash(hash, *a)
      self.name = hash.nil? ? '' : hash['name']
      return self
    end

    def as_json(*a); return to_hash(*a); end
    def to_json(*a); return as_json(*a).to_json; end
  end
end