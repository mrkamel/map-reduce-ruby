module MapReduce
  class HashPartitioner
    def initialize(num_partitions)
      @num_partitions = num_partitions
    end

    def call(key)
      Digest::SHA1.hexdigest(JSON.generate(key))[0..4].to_i(16) % @num_partitions
    end
  end
end
