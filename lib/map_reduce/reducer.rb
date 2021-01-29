module MapReduce
  class Reducer
    include Mergeable
    include Reduceable
    include MonitorMixin

    def initialize(implementation)
      super()

      @implementation = implementation

      @tempfiles ||= []
    end

    def add_chunk
      tempfile = Tempfile.new

      synchronize do
        @tempfiles.push(tempfile)
      end

      tempfile
    end

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
