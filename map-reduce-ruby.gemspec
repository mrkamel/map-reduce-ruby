require_relative "lib/map_reduce/version"

Gem::Specification.new do |spec|
  spec.name          = "map-reduce-ruby"
  spec.version       = MapReduce::VERSION
  spec.authors       = ["Benjamin Vetter"]
  spec.email         = ["vetter@flakks.com"]

  spec.summary       = "The easiest way to write distributed, larger than memory map-reduce jobs"
  spec.description   = "The MapReduce gem is the easiest way to write custom, distributed, larger " \
                       "than memory map-reduce jobs"
  spec.homepage      = "https://github.com/mrkamel/map-reduce-ruby"
  spec.license       = "MIT"

  spec.required_ruby_version = Gem::Requirement.new(">= 2.5.0")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/mrkamel/map-reduce-ruby"
  spec.metadata["changelog_uri"] = "https://github.com/mrkamel/map-reduce/blob/master/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rspec"
  spec.add_development_dependency "rubocop"

  spec.add_dependency "json"
  spec.add_dependency "lazy_priority_queue"

  # For more information and examples about making a new gem, checkout our
  # guide at: https://bundler.io/guides/creating_gem.html
end
