#!/usr/bin/env ruby
# List or redeliver one explicitly identified GitHub App webhook delivery.
# App JWT and private key are never printed.

require "base64"
require "json"
require "net/http"
require "openssl"

root = File.expand_path("..", __dir__)
metadata = JSON.parse(File.read(File.join(root, "data", "secrets", "github-app.json")))
key = OpenSSL::PKey::RSA.new(File.read(File.join(root, "data", "secrets", "github-app.pem")))
now = Time.now.to_i
encode = ->(value) { Base64.urlsafe_encode64(value, padding: false) }
header = encode.call(JSON.generate({ alg: "RS256", typ: "JWT" }))
claims = encode.call(JSON.generate({ iat: now - 60, exp: now + 540, iss: metadata.fetch("id").to_s }))
unsigned = "#{header}.#{claims}"
jwt = "#{unsigned}.#{encode.call(key.sign(OpenSSL::Digest::SHA256.new, unsigned))}"

delivery_id = ARGV.first
path = delivery_id ? "/app/hook/deliveries/#{Integer(delivery_id)}/attempts" : "/app/hook/deliveries?per_page=30"
uri = URI("https://api.github.com#{path}")
request = delivery_id ? Net::HTTP::Post.new(uri) : Net::HTTP::Get.new(uri)
request["accept"] = "application/vnd.github+json"
request["authorization"] = "Bearer #{jwt}"
request["x-github-api-version"] = "2026-03-10"
request["user-agent"] = "powerfarm-antenna-operations"
http = Net::HTTP.new(uri.host, uri.port)
http.use_ssl = true
response = http.start { |client| client.request(request) }

if delivery_id
  abort "redelivery failed with HTTP #{response.code}" unless response.code.to_i == 202
  puts JSON.generate({ delivery_id: Integer(delivery_id), redelivery_request_status: 202 })
else
  abort "listing deliveries failed with HTTP #{response.code}" unless response.code.to_i == 200
  safe = JSON.parse(response.body).map do |delivery|
    delivery.slice("id", "guid", "delivered_at", "redelivery", "duration", "status", "status_code", "event", "action", "installation_id", "repository_id")
  end
  puts JSON.pretty_generate(safe)
end
