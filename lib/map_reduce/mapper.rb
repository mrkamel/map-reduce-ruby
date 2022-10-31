module MapReduce
  # The MapReduce::Mapper class runs the mapping part of your map-reduce job.

  class Mapper
    include Mergeable
    include Reduceable
    include MonitorMixin

    # Initializes a new mapper.
    #
    # @param implementation Your map-reduce implementation, i.e. an object
    #   which responds to #map and #reduce.
    # @param partitioner [#call] A partitioner, i.e. an object which responds
    #   to #call and calculates a partition for the passed key.
    # @param memory_limit [#to_i] The memory limit, i.e. the buffer size in
    #   bytes.
    #
    # @example
    #  MapReduce::Mapper.new(MyImplementation.new, partitioner: HashPartitioner.new(16), memory_limit: 100.megabytes)

    def initialize(implementation, partitioner: HashPartitioner.new(32), memory_limit: 100 * 1024 * 1024)
      super()

      @implementation = implementation
      @partitioner = partitioner
      @memory_limit = memory_limit.to_i

      @buffer_size = 0
      @buffer = []
      @chunks = []
    end

    # Passes the received key to your map-reduce implementation and adds
    # yielded key-value pair to a buffer. When the memory limit is reached, the
    # chunk is sorted and written to a tempfile.
    #
    # @param key The key to pass to the map-reduce implementation.
    #
    # @example
    #   mapper.map("some_key")
    #   mapper.map("other_key")

    def map(*args, **kwargs)
      @implementation.map(*args, **kwargs) do |new_key, new_value|
        synchronize do
          partition = @partitioner.call(new_key)
          item = [[partition, new_key], new_value]

          @buffer.push(item)
          @buffer_size += JSON.generate(item).bytesize

          write_chunk if @buffer_size >= @memory_limit
        end
      end
    end

    # Performs a k-way-merge of the sorted chunks written to tempfiles while
    # already reducing the result using your map-reduce implementation (if
    # available) and splitting the dataset into partitions. Finally yields a
    # hash of (partition, path) pairs containing the data for the partitions
    # in tempfiles.
    #
    # @param chunk_limit [Integer] The maximum number of files to process
    #   at the same time. Most useful when you run on a system where the
    #   number of open file descriptors is limited. If your number of file
    #   descriptors is unlimited, you want to set it to a higher number to
    #   avoid the overhead of multiple runs.
    #
    # @example
    #   mapper.shuffle do |partitions|
    #     partitions.each do |partition, path|
    #       # store data e.g. on s3
    #     end
    #   end

    def shuffle(chunk_limit:)
      raise(InvalidChunkLimit, "Chunk limit must be >= 2") unless chunk_limit >= 2

      write_chunk if @buffer_size > 0

      chunk = k_way_merge(@chunks, chunk_limit: chunk_limit)
      chunk = reduce_chunk(chunk, @implementation) if @implementation.respond_to?(:reduce)

      @partitions = split_partitions(chunk)

      yield(@partitions.transform_values(&:path))

      @chunks.each(&:delete)
      @chunks = []

      @partitions.each_value(&:delete)

      nil
    end

    private

    def split_partitions(chunk)
      res = {}
      current_partition = nil
      file = nil

      chunk.each do |((new_partition, key), value)|
        if new_partition != current_partition
          file&.close

          current_partition = new_partition
          temp_path = TempPath.new
          res[new_partition] = temp_path
          file = File.open(temp_path.path, "w+")
        end

        file.puts(JSON.generate([key, value]))
      end

      file&.close

      res
    end

    def write_chunk
      temp_path = TempPath.new

      @buffer.sort_by!(&:first)

      chunk = @buffer
      chunk = reduce_chunk(chunk, @implementation) if @implementation.respond_to?(:reduce)

      File.open(temp_path.path, "w+") do |file|
        chunk.each do |pair|
          file.puts JSON.generate(pair)
        end
      end

      @chunks.push(temp_path)

      @buffer_size = 0
      @buffer = []
    end
  end
end
