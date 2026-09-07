#!/usr/bin/env ruby
# End-to-end GitHub-shaped edge canary. The webhook secret is read from its
# protected file and never printed or placed in the process argument vector.

require "json"
require "net/http"
require "openssl"
require "securerandom"

if ENV["GITHUB_DNS_SERVER"]
  require "resolv-replace"
  Resolv::DefaultResolver.replace_resolvers([
    Resolv::DNS.new(nameserver: [ENV.fetch("GITHUB_DNS_SERVER")], search: [], ndots: 1)
  ])
end

secret = File.read(File.expand_path("../data/secrets/github-webhook", __dir__)).strip
uri = URI(ENV.fetch("GITHUB_INGRESS_URL", "https://ingress.minilab.work/github"))

def github_signature(secret, body)
  "sha256=#{OpenSSL::HMAC.hexdigest("SHA256", secret, body)}"
end

def send_request(uri, body, headers = {}, method: :post)
  request = method == :post ? Net::HTTP::Post.new(uri) : Net::HTTP::Get.new(uri)
  headers.each { |name, value| request[name] = value }
  request.body = body if method == :post
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = uri.scheme == "https"
  http.start { |client| client.request(request) }
end

def headers(secret, delivery, event, body)
  {
    "content-type" => "application/json",
    "x-github-delivery" => delivery,
    "x-github-event" => event,
    "x-hub-signature-256" => github_signature(secret, body)
  }
end

delivery = "edge-canary-#{SecureRandom.uuid}"
body = JSON.generate({
  action: "edited",
  repository: {
    id: 1_334_511_701,
    full_name: "powerfarm/factory",
    owner: { login: "powerfarm" },
    visibility: "private",
    archived: false,
    default_branch: "main",
    html_url: "https://github.com/powerfarm/factory"
  },
  installation: { id: 0 }
})
valid_headers = headers(secret, delivery, "repository", body)
first = send_request(uri, body, valid_headers)
duplicate = send_request(uri, body, valid_headers)

tamper_delivery = "edge-tamper-#{SecureRandom.uuid}"
tamper_headers = headers(secret, tamper_delivery, "push", "{}")
tampered = send_request(uri, '{"tampered":true}', tamper_headers)

missing_headers = headers(secret, "unused", "push", "{}")
missing_headers.delete("x-github-delivery")
missing = send_request(uri, "{}", missing_headers)

unknown_delivery = "edge-unknown-#{SecureRandom.uuid}"
unknown_body = JSON.generate({ action: "future_action", repository: { id: 1_334_511_701, full_name: "powerfarm/factory" } })
unknown = send_request(uri, unknown_body, headers(secret, unknown_delivery, "future_event", unknown_body))

oversized_body = JSON.generate({ payload: "x" * 1_048_577 })
oversized = send_request(uri, oversized_body, headers(secret, "edge-oversized-#{SecureRandom.uuid}", "push", oversized_body))

results = {
  delivery_id: delivery,
  unknown_delivery_id: unknown_delivery,
  valid_status: first.code.to_i,
  valid_response: JSON.parse(first.body),
  duplicate_status: duplicate.code.to_i,
  duplicate_response: JSON.parse(duplicate.body),
  tampered_status: tampered.code.to_i,
  missing_delivery_status: missing.code.to_i,
  unknown_status: unknown.code.to_i,
  unknown_response: JSON.parse(unknown.body),
  oversized_status: oversized.code.to_i
}

expected = [
  results[:valid_status], results[:duplicate_status], results[:tampered_status],
  results[:missing_delivery_status], results[:unknown_status], results[:oversized_status]
]
abort JSON.generate(results) unless expected == [202, 202, 401, 400, 202, 413]
abort JSON.generate(results) unless results.dig(:duplicate_response, "duplicate") == true
abort JSON.generate(results) unless results.dig(:unknown_response, "classification") == "UNKNOWN"
puts JSON.pretty_generate(results)
