module MapReduce
  module Mergeable
    private

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
