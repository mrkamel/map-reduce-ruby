module MapReduce
  # The MapReduce::Reduceable mixin allows to reduce an arbitrary chunk using
  # the specified map-reduce implementation.

  module Reduceable
    private

    # Reduces the specified chunk, e.g. some enumerable, using the specified
    # map-reduce implementation using a lookahead of one to detect key changes.
    # The reduce implementation is called up until a key change is detected,
    # because the key change signals that the reduce operation is finished for
    # the particular key, such that it will then be yielded.
    #
    # @param chunk The chunk to be reduced. Can e.g. be some enumerable.
    # @param implementation The map-reduce implementation.

    def reduce_chunk(chunk, implementation)
      return enum_for(:reduce_chunk, chunk, implementation) unless block_given?

      last_item = chunk.inject do |prev_item, cur_item|
        prev_key = prev_item[0]

        if prev_key == cur_item[0]
          [prev_key, implementation.reduce(prev_key, prev_item[1], cur_item[1])]
        else
          yield(prev_item)

          cur_item
        end
      end

      yield(last_item) if last_item
    end
  end
end
