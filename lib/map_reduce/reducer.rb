module MapReduce
  # The MapReduce::Reducer class runs the reducer part of your map-reduce job.

  class Reducer
    include Mergeable
    include Reduceable
    include MonitorMixin

    # Initializes a new reducer.
    #
    # @param implementation Your map-reduce implementation, i.e. an object
    #   which response to map and reduce.
    #
    # @example
    #   MapReduce::Reducer.new(MyImplementation.new)

    def initialize(implementation)
      super()

      @implementation = implementation

      @tempfiles ||= []
    end

    # Adds a chunk from the mapper-phase to the reducer by registering and
    # returning a tempfile, such that you can download a chunk e.g. from s3
    # and write it to this tempfile.
    #
    # @example
    #   tempfile = reducer.add_chunk
    #   tempfile.write("downloaded blob")

    def add_chunk
      tempfile = Tempfile.new

      synchronize do
        @tempfiles.push(tempfile)
      end

      tempfile
    end

    # Performs a k-way-merge of the added chunks and yields the reduced
    # key-value pairs.

    def reduce(&block)
      return enum_for(:reduce) unless block_given?

      @tempfiles.each(&:rewind)

      reduce_chunk(k_way_merge(@tempfiles), @implementation).each do |pair|
        block.call(pair)
      end

      @tempfiles.each { |tempfile| tempfile.close(true) }
    end
  end
end
