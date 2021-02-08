module MapReduce
  # The MapReduce::HashPartitioner calculates a partition for the passed keys
  # using SHA1 modulo the desired number of partitions.

  class HashPartitioner
    # Initializes a HashPartitioner.
    #
    # @param num_partitions [Fixnum] The desired number of partitions.
    #   Typically 8, 16, 32, 64, etc. but can be everything according to your
    #   needs.
    #
    # @example
    #   MapReduce::HashPartitioner.new(16)

    def initialize(num_partitions)
      @num_partitions = num_partitions
    end

    # Calculates the partition for the specified key.
    #
    # @param key The key to calculate the partition for. Can be everything
    #   that can be serialized as json.
    # @returns [Integer] The partition number.
    #
    # @example
    #   partitioner.call("some key")

    def call(key)
      Digest::SHA1.hexdigest(JSON.generate(key))[0..4].to_i(16) % @num_partitions
    end
  end
end
