module MapReduce
  # The MapReduce::TempPath generates a tempfile path and automatically deletes
  # the file when the object is garbage collected or manually deleted. Using
  # this class instead of Tempfile allows to have less open file descriptors.

  class TempPath
    attr_reader :path

    # Initializes a new tempfile path.
    #
    # @example
    #   temp_path = MapReduce::TempPath.new
    #   File.write(temp_path.path, "blob")

    def initialize
      @path = Dir::Tmpname.create("") do
        # nothing
      end

      FileUtils.touch(@path)

      ObjectSpace.define_finalizer(self, self.class.finalize(@path))
    end

    # @api private

    def self.finalize(path)
      proc { FileUtils.rm_f(path) }
    end

    # Allows to manually delete the tempfile.
    #
    # @example
    #   temp_path = MapReduce::TempPath.new
    #   File.write(temp_path.path, "blob")
    #   temp_path.delete

    def delete
      FileUtils.rm_f(path)
    end
  end
end
