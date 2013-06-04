require 'net/http'
require 'net/https'
require 'json'

class SkyDB::Connection::Ruby
  ##############################################################################
  #
  # Methods
  #
  ##############################################################################
  
  # Executes a RESTful JSON over HTTP POST.
  def send(options={})
    # Generate a JSON request.
    request = case options[:method]
      when :get then Net::HTTP::Get.new(options[:path])
      when :post then Net::HTTP::Post.new(options[:path])
      when :patch then Net::HTTP::Patch.new(options[:path])
      when :put then Net::HTTP::Put.new(options[:path])
      when :delete then Net::HTTP::Delete.new(options[:path])
      end
    request.add_field('Content-Type', 'application/json')
    request.body = JSON.generate(options[:data], :max_nesting => 200) unless options[:data].nil?

    http = Net::HTTP.new(options[:host], options[:port])
    if options[:ssl]
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE #BAD
    end

    response = http.start {|h| h.request(request) }
    
    # Parse the body as JSON.
    json = JSON.parse(response.body) rescue nil
    message = json['message'] rescue nil
    
    warn("#{options[:method].to_s.upcase} #{options[:path]}: #{request.body} -> #{response.body}") if SkyDB.debug

    # Process based on the response code.
    case response
    when Net::HTTPSuccess then
      return json
    else
      e = SkyDB::Client::ServerError.new(message)
      e.status = response.code
      raise e
    end
  end
end
