RSpec.describe MapReduce::Mapper do
  describe "#map" do
    it "calls the map implementation" do
      implementation = Object.new

      allow(implementation).to receive(:map)

      mapper = described_class.new(implementation)
      mapper.map(key: "value")

      expect(implementation).to have_received(:map).with(key: "value")
    end

    it "writes a chunk to disk when the buffer size is bigger than the memory limit" do
      temp_path1 = MapReduce::TempPath.new
      temp_path2 = MapReduce::TempPath.new

      allow(MapReduce::TempPath).to receive(:new).and_return(temp_path1, temp_path2)

      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield(["key1"], { "value" => "a" * 10 })
        .and_yield(["key2"], { "value" => "b" * 10 })
        .and_yield(["key3"], { "value" => "c" * 10 })
        .and_yield(["key4"], { "value" => "d" * 10 })
        .and_yield(["key5"], { "value" => "e" * 10 })

      mapper = described_class.new(implementation, memory_limit: 50)
      mapper.map("key")

      expect(File.read(temp_path1.path)).to eq(
        [
          JSON.generate([[2, ["key2"]], { "value" => "b" * 10 }]),
          JSON.generate([[31, ["key1"]], { "value" => "a" * 10 }])
        ].join("\n") + "\n"
      )

      expect(File.read(temp_path2.path)).to eq(
        [
          JSON.generate([[0, ["key3"]], { "value" => "c" * 10 }]),
          JSON.generate([[30, ["key4"]], { "value" => "d" * 10 }])
        ].join("\n") + "\n"
      )
    end

    it "sorts and reduces the chunks when writing them" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield(["key3", 1], { "value" => 1 })
        .and_yield(["key3", 3], { "value" => 1 })
        .and_yield(["key3", 1], { "value" => 1 })
        .and_yield(["key1", 1], { "value" => 1 })
        .and_yield(["key2", 1], { "value" => 1 })
        .and_yield(["key1", 1], { "value" => 1 })
        .and_yield(["key3", 11], { "value" => 1 })
        .and_yield(["key3", 2], { "value" => 1 })

      allow(implementation).to receive(:reduce) do |_key, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(2), memory_limit: 90)
      mapper.map("key")

      result = {}

      mapper.shuffle(chunk_limit: 64) do |partitions|
        partitions.each do |partition, path|
          result[partition] = File.open(path).each_line.map { |line| JSON.parse(line) }
        end
      end

      expect(result).to eq(
        0 => [
          [["key1", 1], { "value" => 2 }],
          [["key2", 1], { "value" => 1 }],
          [["key3", 3], { "value" => 1 }],
          [["key3", 11], { "value" => 1 }]
        ],
        1 => [
          [["key3", 1], { "value" => 2 }],
          [["key3", 2], { "value" => 1 }]
        ]
      )
    end

    it "only sorts, but does not reduce the chunks when there is no reduce implementation" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield(["key3", 1], { "value" => 1 })
        .and_yield(["key3", 3], { "value" => 1 })
        .and_yield(["key1", 1], { "value" => 1 })
        .and_yield(["key2", 1], { "value" => 1 })
        .and_yield(["key1", 1], { "value" => 1 })
        .and_yield(["key3", 11], { "value" => 1 })
        .and_yield(["key3", 2], { "value" => 1 })

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(2))
      mapper.map("key")

      result = {}

      mapper.shuffle(chunk_limit: 64) do |partitions|
        partitions.each do |partition, path|
          result[partition] = File.open(path).each_line.map { |line| JSON.parse(line) }
        end
      end

      expect(result).to eq(
        0 => [
          [["key1", 1], { "value" => 1 }],
          [["key1", 1], { "value" => 1 }],
          [["key2", 1], { "value" => 1 }],
          [["key3", 3], { "value" => 1 }],
          [["key3", 11], { "value" => 1 }]
        ],
        1 => [
          [["key3", 1], { "value" => 1 }],
          [["key3", 2], { "value" => 1 }]
        ]
      )
    end
  end

  describe "#shuffle" do
    it "merges the chunks and yields the partitions with tempfiles" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield(["key1"], { "value" => "a" * 10 })
        .and_yield(["key2"], { "value" => "b" * 10 })
        .and_yield(["key3"], { "value" => "c" * 10 })
        .and_yield(["key4"], { "value" => "d" * 10 })
        .and_yield(["key5"], { "value" => "e" * 10 })

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(4), memory_limit: 100)
      mapper.map("key")

      result = {}

      mapper.shuffle(chunk_limit: 64) do |partitions|
        partitions.each do |partition, path|
          result[partition] = File.open(path).each_line.map { |line| JSON.parse(line) }
        end
      end

      expect(result).to eq(
        0 => [
          [["key3"], { "value" => "c" * 10 }]
        ],
        2 => [
          [["key2"], { "value" => "b" * 10 }],
          [["key4"], { "value" => "d" * 10 }]
        ],
        3 => [
          [["key1"], { "value" => "a" * 10 }],
          [["key5"], { "value" => "e" * 10 }]
        ]
      )
    end

    it "reduces each partition" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield(["key3"], { "value" => 1 })
        .and_yield(["key1"], { "value" => 1 })
        .and_yield(["key2"], { "value" => 1 })
        .and_yield(["key1"], { "value" => 1 })
        .and_yield(["key3"], { "value" => 1 })
        .and_yield(["key1"], { "value" => 1 })

      allow(implementation).to receive(:reduce) do |_, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(2), memory_limit: 40)
      mapper.map("key")

      result = {}

      mapper.shuffle(chunk_limit: 64) do |partitions|
        partitions.each do |partition, path|
          result[partition] = File.open(path).each_line.map { |line| JSON.parse(line) }
        end
      end

      expect(result).to eq(
        0 => [[["key2"], { "value" => 1 }], [["key3"], { "value" => 2 }]],
        1 => [[["key1"], { "value" => 3 }]]
      )
    end

    it "does not neccessarily need a reduce implementation when there is nothing to reduce" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield("key3", { "value" => 1 })
        .and_yield("key1", { "value" => 1 })
        .and_yield("key2", { "value" => 1 })

      allow(implementation).to receive(:reduce).and_raise(NotImplementedError)

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(8), memory_limit: 40)
      mapper.map("key")

      result = {}

      mapper.shuffle(chunk_limit: 64) do |partitions|
        partitions.each do |partition, path|
          result[partition] = File.open(path).each_line.map { |line| JSON.parse(line) }
        end
      end

      expect(result).to eq(
        1 => [["key1", { "value" => 1 }]],
        2 => [["key2", { "value" => 1 }]],
        6 => [["key3", { "value" => 1 }]]
      )
    end

    it "raises a InvalidChunkLimit error when chunk_limit is less than 2" do
      expect { described_class.new(Object.new).shuffle(chunk_limit: 1, &proc {}) }.to raise_error(MapReduce::InvalidChunkLimit)
    end

    it "raises errors when e.g. the disk is full after cleanup" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield(["key1"], { "value" => 1 })
        .and_yield(["key2"], { "value" => 1 })
        .and_yield(["key3"], { "value" => 1 })

      mapper = described_class.new(implementation)
      mapper.map("key")

      allow(mapper).to receive(:split_chunk).and_raise("error")

      expect do
        mapper.shuffle(chunk_limit: 64) do
          # nothing
        end
      end.to raise_error("error")
    end
  end
end
