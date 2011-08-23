require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "em-postgres"
    gemspec.summary = "Async PostgreSQL driver for Ruby/Eventmachine"
    gemspec.description = gemspec.summary
    gemspec.email = "jtoy@jtoy.net"
    gemspec.homepage = "http://github.com/jtoy/em-postgres"
    gemspec.authors = ["Jason Toy"]
    gemspec.add_dependency('eventmachine', '>= 0.12.9')
    gemspec.rubyforge_project = "em-postgres"
  end

  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: sudo gem install technicalpickles-jeweler -s http://gems.github.com"
end