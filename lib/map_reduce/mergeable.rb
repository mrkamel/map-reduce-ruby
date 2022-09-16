module MapReduce
  # The MapReduce::Mergeable mixin provides the k-way-merge operation used by
  # mappers as well as reducers.

  module Mergeable
    private

    # Performs the k-way-merge of the passed files using a priority queue using
    # a binomial heap. The content of the passed files needs to be sorted. It
    # starts by reading one item of each file and adding it to the priority
    # queue. Afterwards, it continously pops an item from the queue, yields it
    # and reads a new item from the file the popped item belongs to, adding the
    # read item to the queue. This continues up until all items from the files
    # have been read. This guarantees that the yielded key-value pairs are
    # sorted without having all items in-memory.
    #
    # @param files [IO, Tempfile] The files to run the k-way-merge for. The
    #   content of the files must be sorted.

    def k_way_merge(files)
      return enum_for(:k_way_merge, files) unless block_given?

      queue = PriorityQueue.new

      files.each_with_index do |file, index|
        line = file.eof? ? nil : file.readline

        next unless line

        key, value = JSON.parse(line)

        queue.push([key, value, index], key)
      end

      loop do
        key, value, index = queue.pop

        return unless index

        yield([key, value])

        line = files[index].yield_self { |file| file.eof? ? nil : file.readline }

        next unless line

        key, value = JSON.parse(line)

        queue.push([key, value, index], key)
      end

      files.each(&:rewind)

      nil
    end
  end
end
