#!/usr/bin/env ruby
# Local operational canary. Secrets are read from the protected Antenna config
# and are never printed or written to an argument vector.

require "digest"
require "json"
require "net/http"
require "openssl"
require "securerandom"
require "time"

config = File.read(File.expand_path("../antenna.toml", __dir__))
handoff_secret = config[/^handoff_secret\s*=\s*"([^"]+)"/, 1] or abort "handoff secret missing"
inspect_token = config[/^inspect_token\s*=\s*"([^"]+)"/, 1] or abort "inspection token missing"
uri = URI(ENV.fetch("ANTENNA_CANARY_URL", "http://127.0.0.1:8799/internal/github"))

def signed_headers(secret, delivery, event, body, timestamp)
  digest = Digest::SHA256.hexdigest(body)
  canonical = "#{timestamp}\n#{delivery}\n#{event}\n#{digest}\n"
  signature = OpenSSL::HMAC.hexdigest("SHA256", secret, canonical)
  {
    "content-type" => "application/json",
    "x-antenna-timestamp" => timestamp.to_s,
    "x-antenna-delivery" => delivery,
    "x-antenna-github-event" => event,
    "x-antenna-body-sha256" => digest,
    "x-antenna-signature" => "v1=#{signature}"
  }
end

def send_request(uri, headers, body)
  request = Net::HTTP::Post.new(uri)
  headers.each { |name, value| request[name] = value }
  request.body = body
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = uri.scheme == "https"
  http.start { |client| client.request(request) }
end

delivery = "local-canary-#{SecureRandom.uuid}"
body = JSON.generate({
  action: "created",
  repository: {
    id: 1_334_511_701,
    full_name: "powerfarm/factory",
    visibility: "public",
    archived: false,
    default_branch: "main"
  },
  installation: { id: 0 },
  after: "0123456789abcdef0123456789abcdef01234567"
})
headers = signed_headers(handoff_secret, delivery, "repository", body, Time.now.to_i)
first = send_request(uri, headers, body)
duplicate = send_request(uri, headers, body)

tamper_delivery = "local-tamper-#{SecureRandom.uuid}"
original = "{}"
tampered_headers = signed_headers(handoff_secret, tamper_delivery, "push", original, Time.now.to_i)
tampered = send_request(uri, tampered_headers, '{"tampered":true}')

stale_delivery = "local-stale-#{SecureRandom.uuid}"
stale_headers = signed_headers(handoff_secret, stale_delivery, "push", "{}", Time.now.to_i - 3600)
stale = send_request(uri, stale_headers, "{}")

unknown_delivery = "local-unknown-#{SecureRandom.uuid}"
unknown_body = JSON.generate({ action: "future_action", repository: { id: 1_334_511_701, full_name: "powerfarm/factory" } })
unknown_headers = signed_headers(handoff_secret, unknown_delivery, "future_event", unknown_body, Time.now.to_i)
unknown = send_request(uri, unknown_headers, unknown_body)

status_uri = URI("http://127.0.0.1:8799/status")
status_request = Net::HTTP::Get.new(status_uri)
status_request["authorization"] = "Bearer #{inspect_token}"
status = Net::HTTP.start(status_uri.host, status_uri.port) { |http| http.request(status_request) }

results = {
  delivery_id: delivery,
  unknown_delivery_id: unknown_delivery,
  valid_status: first.code.to_i,
  valid_response: JSON.parse(first.body),
  duplicate_status: duplicate.code.to_i,
  duplicate_response: JSON.parse(duplicate.body),
  tampered_status: tampered.code.to_i,
  stale_status: stale.code.to_i,
  unknown_status: unknown.code.to_i,
  unknown_response: JSON.parse(unknown.body),
  operational_status: JSON.parse(status.body)
}

expected = [first.code.to_i, duplicate.code.to_i, tampered.code.to_i, stale.code.to_i, unknown.code.to_i]
abort JSON.generate(results) unless expected == [202, 202, 401, 401, 202]
abort JSON.generate(results) unless results.dig(:duplicate_response, "duplicate") == true
abort JSON.generate(results) unless results.dig(:unknown_response, "classification") == "UNKNOWN"
puts JSON.pretty_generate(results)
