module MapReduce
  # The MapReduce::Mergeable mixin provides the k-way-merge operation used by
  # mappers as well as reducers.

  module Mergeable
    private

    # Performs the k-way-merge of the passed files referenced by the temp paths
    # using a priority queue using a binomial heap. The content of the passed
    # files needs to be sorted. It starts by reading one item of each file and
    # adding it to the priority queue. Afterwards, it continously pops an item
    # from the queue, yields it and reads a new item from the file the popped
    # item belongs to, adding the read item to the queue. This continues up
    # until all items from the files have been read. This guarantees that the
    # yielded key-value pairs are sorted without having all items in-memory.
    #
    # @param temp_paths [TempPath] The files referenced by the temp paths to
    #   run the k-way-merge for. The content of the files must be sorted.
    # @param chunk_limit [Integer] The maximum number of files to process
    #   at the same time. Most useful when you run on a system where the
    #   number of open file descriptors is limited. If your number of file
    #   descriptors is unlimited, you want to set it to a higher number to
    #   avoid the overhead of multiple runs.

    def k_way_merge(temp_paths, chunk_limit:, &block)
      return enum_for(__method__, temp_paths, chunk_limit: chunk_limit) unless block_given?

      dupped_temp_paths = temp_paths.dup
      additional_temp_paths = []

      while dupped_temp_paths.size > chunk_limit
        temp_path_out = TempPath.new

        File.open(temp_path_out.path, "w+") do |file|
          files = dupped_temp_paths.shift(chunk_limit).map { |temp_path| File.open(temp_path.path, "r") }

          k_way_merge!(files) do |pair|
            file.puts(JSON.generate(pair))
          end

          files.each(&:close)
        end

        dupped_temp_paths.push(temp_path_out)
        additional_temp_paths.push(temp_path_out)
      end

      files = dupped_temp_paths.map { |temp_path| File.open(temp_path.path, "r") }
      k_way_merge!(files, &block)
      files.each(&:close)

      nil
    ensure
      additional_temp_paths&.each(&:delete)
    end

    # Performs the actual k-way-merge of the specified files.
    #
    # @param files [IO, Tempfile] The files to run the k-way-merge for.
    #   The content of the files must be sorted.

    def k_way_merge!(files)
      return enum_for(__method__, files) unless block_given?

      if files.size == 1
        files.first.each_line do |line|
          yield(JSON.parse(line))
        end

        files.each(&:rewind)

        return
      end

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
