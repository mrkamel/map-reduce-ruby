module MapReduce
  class TempPath
    attr_reader :path

    def initialize
      @path = Dir::Tmpname.create("") do
        # nothing
      end

      FileUtils.touch(@path)

      ObjectSpace.define_finalizer(self, self.class.finalize(@path))
    end

    def self.finalize(path)
      proc { FileUtils.rm_f(path) }
    end

    def delete
      FileUtils.rm_f(path)
    end
  end
end
