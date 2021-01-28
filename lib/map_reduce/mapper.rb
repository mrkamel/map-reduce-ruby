module MapReduce
  class Mapper
    include Mergeable
    include Reduceable
    include MonitorMixin

    attr_reader :partitions

    def initialize(implementation, partitioner: HashPartitioner.new(32), memory_limit: 100 * 1024 * 1024)
      super()

      @implementation = implementation
      @partitioner = partitioner
      @memory_limit = memory_limit.to_i

      @buffer_size = 0
      @buffer = []
      @chunks = []
    end

    def map(key)
      @implementation.map(key) do |new_key, new_value|
        synchronize do
          @buffer.push([new_key, new_value])

          @buffer_size += new_key.inspect.bytesize + new_value.inspect.bytesize

          write_chunk if @buffer_size >= @memory_limit
        end
      end
    end

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
