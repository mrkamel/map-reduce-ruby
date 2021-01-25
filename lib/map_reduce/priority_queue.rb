module MapReduce
  class PriorityQueue
    def initialize
      @queue = MinPriorityQueue.new
      @sequence_number = 0
    end

    def push(object, key)
      @queue.push([@sequence_number, object], key)

      @sequence_number += 1
    end

    def pop
      _, object = @queue.pop

      object
    end
  end
end
