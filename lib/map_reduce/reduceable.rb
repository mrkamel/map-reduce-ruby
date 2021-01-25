module MapReduce
  module Reduceable
    private

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
