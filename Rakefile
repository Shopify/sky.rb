require 'rubygems'
require 'rubygems/package_task'
require 'rake/testtask'

$:.unshift File.join(File.dirname(__FILE__), 'lib')
require 'skydb/version'


#############################################################################
#
# Testing tasks
#
#############################################################################

task :test    => ['test:unit', 'test:integration']

namespace :test do
  Rake::TestTask.new(:unit) do |t|
    t.options = "-v"
    t.libs << "test"
    t.test_files = FileList["test/unit/*_test.rb", "test/unit/**/*_test.rb"]
  end

  Rake::TestTask.new(:integration) do |t|
    t.options = "-v"
    t.libs << "test"
    t.test_files = FileList["test/integration/*_test.rb", "test/integration/**/*_test.rb"]
  end
end


#############################################################################
#
# Utility tasks
#
#############################################################################

task :console do
  sh "irb -I lib -r skydb"
end


#############################################################################
#
# Packaging tasks
#
#############################################################################

task :release do
  puts ""
  print "Are you sure you want to relase Sky #{SkyDB::VERSION}? [y/N] "
  exit unless STDIN.gets.index(/y/i) == 0
  
  unless `git branch` =~ /^\* master$/
    puts "You must be on the master branch to release!"
    exit!
  end
  
  # Build gem and upload
  sh "gem build skydb.gemspec"
  sh "gem push skydb-#{SkyDB::VERSION}.gem"
  sh "rm skydb-#{SkyDB::VERSION}.gem"
  
  # Commit
  sh "git commit --allow-empty -a -m 'v#{SkyDB::VERSION}'"
  sh "git tag v#{SkyDB::VERSION}"
  sh "git push origin master"
  sh "git push origin v#{SkyDB::VERSION}"
end
