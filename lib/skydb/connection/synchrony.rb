require 'json'

class SkyDB::Connection::EventMachine
  ##############################################################################
  #
  # Methods
  #
  ##############################################################################
  
  # Executes a RESTful JSON over HTTP POST using EventMachine async callbacks.
  def send(options={})
    # Setup connection parameters.
    scheme = "http#{options[:ssl] ? 's' : ''}"
    url = "#{scheme}://#{options[:host]}:#{options[:port]}#{options[:path]}"
    req_options = {:head => {'Content-Type' => 'application/json'}}
    req_options[:body] = JSON.generate(options[:data], :max_nesting => 200) unless options[:data].nil?

    status, response = 0, nil
    # Generate a JSON request.
    http = EventMachine::HttpRequest.new(url)
    request = case options[:method]
      when :get then EM::Synchrony.sync http.get(req_options)
      when :post then EM::Synchrony.sync http.post(req_options)
      when :patch then EM::Synchrony.sync http.patch(req_options)
      when :delete then EM::Synchrony.sync http.delete(req_options)
      end
    status, response = request.response_header.status, request.response

    # Parse the body as JSON.
    json = JSON.parse(response) rescue nil
    message = json['message'] rescue nil

    # Process based on the response code.
    case status
    when 200 then
      return json
    else
      e = SkyDB::Client::ServerError.new(message)
      e.status = status
      raise e
    end
  end
end
