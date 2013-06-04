require 'date'
require 'net/http'
require 'json'

class SkyDB
  ############################################################################
  #
  # Errors
  #
  ############################################################################

  class SkyError < StandardError; end

  ############################################################################
  #
  # Static Attributes
  #
  ############################################################################

  ######################################
  # Debugging
  ######################################

  class << self
    attr_accessor :debug
  end


  ############################################################################
  #
  # Static Methods
  #
  ############################################################################

  ######################################
  # Timestamps
  ######################################
  
  # Formats a timestamp as ISO8601 formatted string with fractional seconds.
  def self.format_timestamp(timestamp)
    if timestamp.nil?
      return nil
    else
      return timestamp.to_time.utc.to_datetime.strftime('%Y-%m-%dT%H:%M:%S.%6NZ')
    end
  end
end

require 'skydb/client'
require 'skydb/connection'
require 'skydb/table'
require 'skydb/property'
require 'skydb/event'
require 'skydb/version'
