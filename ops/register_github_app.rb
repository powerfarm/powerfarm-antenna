#!/usr/bin/env ruby
# Registers the dedicated observer through GitHub's official App Manifest flow.
# Generated secrets are written mode 0600 and never printed.

require "cgi"
require "json"
require "net/http"
require "securerandom"
require "uri"
require "webrick"

root = File.expand_path("..", __dir__)
secret_dir = File.join(root, "data", "secrets")
state = SecureRandom.hex(24)
port = 8977
callback = "http://127.0.0.1:#{port}/callback"
manifest = {
  name: "PowerFarm Antenna Observer",
  url: "https://github.com/powerfarm",
  description: "Least-privilege repository lifecycle observation and census for LAB-8GB Antenna.",
  hook_attributes: {
    url: "https://ingress.minilab.work/github",
    active: true
  },
  redirect_url: callback,
  public: false,
  request_oauth_on_install: false,
  setup_on_update: false,
  default_permissions: {
    metadata: "read"
  },
  default_events: ["repository"]
}

logger = WEBrick::Log.new(File::NULL, WEBrick::Log::FATAL)
server = WEBrick::HTTPServer.new(
  BindAddress: "127.0.0.1",
  Port: port,
  Logger: logger,
  AccessLog: []
)

server.mount_proc "/" do |_request, response|
  action = "https://github.com/organizations/powerfarm/settings/apps/new?state=#{CGI.escape(state)}"
  response["content-type"] = "text/html; charset=utf-8"
  response.body = <<~HTML
    <!doctype html><meta charset="utf-8">
    <title>Register PowerFarm Antenna Observer</title>
    <h1>Register PowerFarm Antenna Observer</h1>
    <p>This requests repository Metadata read only and subscribes only to repository lifecycle events.</p>
    <form action="#{CGI.escapeHTML(action)}" method="post">
      <input type="hidden" name="manifest" value="#{CGI.escapeHTML(JSON.generate(manifest))}">
      <button type="submit">Continue to GitHub</button>
    </form>
  HTML
end

server.mount_proc "/callback" do |request, response|
  unless request.query["state"] == state && request.query["code"]
    response.status = 400
    response.body = "State or manifest code missing; no credential was written."
    next
  end
  uri = URI("https://api.github.com/app-manifests/#{CGI.escape(request.query["code"])}/conversions")
  exchange = Net::HTTP::Post.new(uri)
  exchange["accept"] = "application/vnd.github+json"
  exchange["x-github-api-version"] = "2026-03-10"
  exchange["user-agent"] = "powerfarm-antenna-app-registration"
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  result = http.start { |client| client.request(exchange) }
  unless result.code.to_i == 201
    response.status = 502
    response.body = "GitHub manifest conversion failed with HTTP #{result.code}; no credential was written."
    next
  end
  value = JSON.parse(result.body)
  required = %w[id slug pem webhook_secret]
  unless required.all? { |key| value[key] && !value[key].to_s.empty? }
    response.status = 502
    response.body = "GitHub conversion omitted required fields; no credential was written."
    next
  end
  Dir.mkdir(secret_dir, 0700) unless Dir.exist?(secret_dir)
  {
    "github-app.pem" => value.fetch("pem"),
    "github-app-webhook-secret" => value.fetch("webhook_secret"),
    "github-app.json" => JSON.pretty_generate({
      id: value.fetch("id"),
      slug: value.fetch("slug"),
      name: value["name"],
      owner: value.dig("owner", "login"),
      permissions: value["permissions"],
      events: value["events"],
      created_at: value["created_at"]
    })
  }.each do |name, contents|
    File.open(File.join(secret_dir, name), File::WRONLY | File::CREAT | File::EXCL, 0600) do |file|
      file.write(contents)
      file.write("\n") unless contents.end_with?("\n")
    end
  end
  install_url = "https://github.com/apps/#{value.fetch("slug")}/installations/new"
  response["content-type"] = "text/html; charset=utf-8"
  response.body = <<~HTML
    <!doctype html><meta charset="utf-8">
    <title>PowerFarm observer registered</title>
    <h1>App registered</h1>
    <p>App ID #{value.fetch("id")} was registered under #{CGI.escapeHTML(value.dig("owner", "login").to_s)}.</p>
    <p><a href="#{CGI.escapeHTML(install_url)}">Install it on the powerfarm organization</a>, selecting all repositories so future repositories are included.</p>
  HTML
  Thread.new { sleep 2; server.shutdown }
end

trap("INT") { server.shutdown }
puts "GitHub App manifest listener ready on http://127.0.0.1:#{port}/"
system("open", "http://127.0.0.1:#{port}/")
server.start
