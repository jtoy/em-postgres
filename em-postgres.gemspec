spec = Gem::Specification.new do |s|
  s.name = 'em-postgres'
  s.version = '0.0.1'
  s.date = '2011-08-20'
  s.summary = 'Async PostgreSQL client API for Ruby/EventMachine'
  s.email = "jtoy@jtoy.net"
  s.homepage = "http://github.com/jtoy/em-postgres"
  s.description = 'Async PostgreSQL client API for Ruby/EventMachine'
  s.has_rdoc = false
  s.authors = ["Jason Toy"]
  s.add_dependency('eventmachine', '>= 0.12.9')

  # git ls-files
  s.files = %w[
    README
    em-postgres.gemspec
    lib/em/postgres.rb
    test.rb
  ]
end
