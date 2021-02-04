module MapReduce
  # The MapReduce::Mapper class runs the mapping part of your map-reduce job.

  class Mapper
    include Mergeable
    include Reduceable
    include MonitorMixin

    attr_reader :partitions

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

    def map(key)
      @implementation.map(key) do |new_key, new_value|
        synchronize do
          @buffer.push([new_key, new_value])

          @buffer_size += JSON.generate([new_key, new_value]).bytesize

          write_chunk if @buffer_size >= @memory_limit
        end
      end
    end

    # Performs a k-way-merge of the sorted chunks written to tempfiles while
    # already reducing the result using your map-reduce implementation and
    # splitting the dataset into partitions. Finally yields each partition with
    # the tempfile containing the data of the partition.
    #
    # @example
    #   mapper.shuffle do |partition, tempfile|
    #     # store data e.g. on s3
    #   end

    def shuffle(&block)
      return enum_for(:shuffle) unless block_given?

      write_chunk if @buffer_size > 0

      partitions = {}

      reduce_chunk(k_way_merge(@chunks), @implementation).each do |pair|
        partition = @partitioner.call(pair[0])

        (partitions[partition] ||= Tempfile.new).puts(JSON.generate(pair))
      end

      @chunks.each { |tempfile| tempfile.close(true) }
      @chunks = []

      partitions.each_value(&:rewind)

      partitions.each do |partition, tempfile|
        block.call(partition, tempfile)
      end

      partitions.each_value { |tempfile| tempfile.close(true) }

      nil
    end

    private

    def write_chunk
      tempfile = Tempfile.new

      @buffer.sort_by!(&:first)

      reduce_chunk(@buffer, @implementation).each do |pair|
        tempfile.puts JSON.generate(pair)
      end

      tempfile.rewind

      @chunks.push(tempfile)

      @buffer_size = 0
      @buffer = []
    end
  end
end
