$:.unshift(File.dirname(__FILE__) + '/../lib')

require "eventmachine"

%w[ postgres connection ].each do |file|
  require "em-postgres/#{file}"
end
