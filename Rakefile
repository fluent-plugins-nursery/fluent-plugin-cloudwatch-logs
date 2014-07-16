require "bundler/gem_tasks"

require 'rake/testtask'

Rake::TestTask.new(:test) do |test|
  test.libs << 'test'
  test.test_files = FileList['test/plugin/*.rb']
end

