module MapReduce
  # The MapReduce::PriorityQueue implements a min priority queue using a
  # binomial heap.

  class PriorityQueue
    # Initializes the priority queue.
    #
    # @example
    #   MapReduce::PriorityQueue.new

    def initialize
      @queue = MinPriorityQueue.new
      @sequence_number = 0
    end

    # Adds a new item to the priority queue while the key is used for sorting.
    # The object and key can basically be everything, but the key must be some
    # comparable object.
    #
    # @param object The object to add to the priority queue.
    # @param key The key to use for sorting
    #
    # @example
    #   priority_queue = MapReduce::PriorityQueue.new
    #   priority_queue.push("some object", "some key")

    def push(object, key)
      @queue.push([@sequence_number, object], key)

      @sequence_number += 1
    end

    # Pops the min item from the queue.
    #
    # @example
    #   priority_queue = MapReduce::PriorityQueue.new
    #   priority_queue.push("object1", "key1")
    #   priority_queue.push("object2", "key2")
    #   priority_queue.pop

    def pop
      _, object = @queue.pop

      object
    end
  end
end
