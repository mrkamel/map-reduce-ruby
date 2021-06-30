module MapReduce
  # The MapReduce::Reducer class runs the reducer part of your map-reduce job.

  class Reducer
    include Mergeable
    include Reduceable
    include MonitorMixin

    class InvalidChunkLimit < StandardError; end

    # Initializes a new reducer.
    #
    # @param implementation Your map-reduce implementation, i.e. an object
    #   which responds to #map and #reduce.
    #
    # @example
    #   MapReduce::Reducer.new(MyImplementation.new)

    def initialize(implementation)
      super()

      @implementation = implementation

      @temp_paths ||= []
    end

    # Adds a chunk from the mapper-phase to the reducer by registering and
    # returning a tempfile, such that you can download a chunk e.g. from s3
    # and write the content to this tempfile.
    #
    # @returns [String] The newly created tempfile path.
    #
    # @example
    #   chunk_path = reducer.add_chunk
    #   File.write(chunk_path, "downloaded blob")

    def add_chunk
      temp_path = TempPath.new

      synchronize do
        @temp_paths.push(temp_path)
      end

      temp_path.path
    end

    # Performs a k-way-merge of the added chunks and yields the reduced
    # key-value pairs. It performs multiple runs when more than `chunk_limit`
    # chunks exist. A run means: it takes up to `chunk_limit` chunks,
    # reduces them and pushes the result as a new chunk. At the end it
    # removes all tempfiles, even if errors occur.
    #
    # @param chunk_limit [Integer] The maximum number of files to process
    #   during a single run. Most useful when you run on a system where the
    #   number of open file descriptors is limited. If your number of file
    #   descriptors is unlimited, you want to set it to a higher number to
    #   avoid the overhead of multiple runs.
    #
    # @example
    #   reducer = MapReduce::Reducer.new(MyImplementation.new)
    #
    #   chunk1_path = reducer.add_chunk
    #   # write data to the file
    #
    #   chunk2_path = reducer.add_chunk
    #   # write data to the file
    #
    #   reducer.reduce(chunk_limit: 32) do |key, value|
    #     # ...
    #   end

    def reduce(chunk_limit:, &block)
      return enum_for(:reduce, chunk_limit: chunk_limit) unless block_given?

      raise(InvalidChunkLimit, "Chunk limit must be >= 2") unless chunk_limit >= 2

      begin
        loop do
          slice = @temp_paths.shift(chunk_limit)
          files = slice.select { |temp_path| File.exist?(temp_path.path) }
                       .map { |temp_path| File.open(temp_path.path, "r") }

          begin
            if @temp_paths.empty?
              reduce_chunk(k_way_merge(files), @implementation).each do |pair|
                block.call(pair)
              end

              return
            end

            File.open(add_chunk, "w") do |file|
              reduce_chunk(k_way_merge(files), @implementation).each do |pair|
                file.puts JSON.generate(pair)
              end
            end
          ensure
            files.each(&:close)
            slice.each(&:delete)
          end
        end
      ensure
        @temp_paths.each(&:delete)
      end
    end
  end
end
