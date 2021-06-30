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
      tempfile1 = Tempfile.new
      tempfile2 = Tempfile.new

      allow(Tempfile).to receive(:new).and_return(tempfile1, tempfile2)

      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield({ "key" => "key1" }, { "value" => "a" * 10 })
        .and_yield({ "key" => "key2" }, { "value" => "b" * 10 })
        .and_yield({ "key" => "key3" }, { "value" => "c" * 10 })
        .and_yield({ "key" => "key4" }, { "value" => "d" * 10 })
        .and_yield({ "key" => "key5" }, { "value" => "e" * 10 })

      mapper = described_class.new(implementation, memory_limit: 50)
      mapper.map("key")

      expect(tempfile1.tap(&:rewind).read).to eq(
        [
          JSON.generate([{ "key" => "key1" }, { "value" => "a" * 10 }]),
          JSON.generate([{ "key" => "key2" }, { "value" => "b" * 10 }])
        ].join("\n") + "\n"
      )

      expect(tempfile2.tap(&:rewind).read).to eq(
        [
          JSON.generate([{ "key" => "key3" }, { "value" => "c" * 10 }]),
          JSON.generate([{ "key" => "key4" }, { "value" => "d" * 10 }])
        ].join("\n") + "\n"
      )
    end

    it "sorts and reduces the chunks when writing them" do
      tempfile = Tempfile.new

      allow(Tempfile).to receive(:new).and_return(tempfile)

      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield({ "key" => "key3" }, { "value" => 1 })
        .and_yield({ "key" => "key1" }, { "value" => 1 })
        .and_yield({ "key" => "key2" }, { "value" => 1 })
        .and_yield({ "key" => "key1" }, { "value" => 1 })

      allow(implementation).to receive(:reduce) do |_key, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      mapper = described_class.new(implementation, memory_limit: 90)
      mapper.map("key")

      expect(tempfile.tap(&:rewind).read).to eq(
        [
          JSON.generate([{ "key" => "key1" }, { "value" => 2 }]),
          JSON.generate([{ "key" => "key2" }, { "value" => 1 }]),
          JSON.generate([{ "key" => "key3" }, { "value" => 1 }])
        ].join("\n") + "\n"
      )
    end
  end

  describe "#shuffle" do
    it "merges the chunks and yields the partitions with tempfiles" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield({ "key" => "key1" }, { "value" => "a" * 10 })
        .and_yield({ "key" => "key2" }, { "value" => "b" * 10 })
        .and_yield({ "key" => "key3" }, { "value" => "c" * 10 })
        .and_yield({ "key" => "key4" }, { "value" => "d" * 10 })
        .and_yield({ "key" => "key5" }, { "value" => "e" * 10 })

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(4), memory_limit: 100)
      mapper.map("key")

      result = mapper.shuffle.map { |partition, tempfile| [partition, tempfile.read] }

      expect(result).to eq(
        [
          [
            3,
            [
              JSON.generate([{ "key" => "key1" }, { "value" => "a" * 10 }]),
              JSON.generate([{ "key" => "key5" }, { "value" => "e" * 10 }])
            ].join("\n") + "\n"
          ],
          [
            1,
            [
              JSON.generate([{ "key" => "key2" }, { "value" => "b" * 10 }]),
              JSON.generate([{ "key" => "key3" }, { "value" => "c" * 10 }])
            ].join("\n") + "\n"
          ],
          [
            0,
            [
              JSON.generate([{ "key" => "key4" }, { "value" => "d" * 10 }])
            ].join("\n") + "\n"
          ]
        ]
      )
    end

    it "reduces each partition" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield({ "key" => "key3" }, { "value" => 1 })
        .and_yield({ "key" => "key1" }, { "value" => 1 })
        .and_yield({ "key" => "key2" }, { "value" => 1 })
        .and_yield({ "key" => "key1" }, { "value" => 1 })
        .and_yield({ "key" => "key3" }, { "value" => 1 })
        .and_yield({ "key" => "key1" }, { "value" => 1 })

      allow(implementation).to receive(:reduce) do |_, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(8), memory_limit: 40)
      mapper.map("key")

      result = mapper.shuffle.map { |partition, tempfile| [partition, tempfile.read] }

      expect(result).to eq(
        [
          [3, JSON.generate([{ "key" => "key1" }, { "value" => 3 }]) + "\n"],
          [5, JSON.generate([{ "key" => "key2" }, { "value" => 1 }]) + "\n"],
          [1, JSON.generate([{ "key" => "key3" }, { "value" => 2 }]) + "\n"]
        ]
      )
    end
  end
end
