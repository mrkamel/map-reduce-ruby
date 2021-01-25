module MapReduce
  class Reducer
    include Mergeable
    include Reduceable

    def initialize(implementation)
      @implementation = implementation

      @tempfiles ||= []
    end

    def add_chunk
      tempfile = Tempfile.new

      @tempfiles.push(tempfile)

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
