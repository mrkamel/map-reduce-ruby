RSpec.describe MapReduce::Mapper do
  describe "#map" do
    it "calls the map implementation" do
      implementation = Object.new

      allow(implementation).to receive(:map)

      mapper = described_class.new(implementation)
      mapper.map("key")

      expect(implementation).to have_received(:map).with("key")
    end

    it "writes a chunk to disk when the buffer size is bigger than the memory limit" do
      tempfile1 = Tempfile.new
      tempfile2 = Tempfile.new

      allow(Tempfile).to receive(:new).and_return(tempfile1, tempfile2)

      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield("key1", "a" * 10)
        .and_yield("key2", "b" * 10)
        .and_yield("key3", "c" * 10)
        .and_yield("key4", "d" * 10)
        .and_yield("key5", "e" * 10)

      mapper = described_class.new(implementation, memory_limit: 25)
      mapper.map("key")

      expect(tempfile1.tap(&:rewind).read).to eq("[\"key1\",\"aaaaaaaaaa\"]\n[\"key2\",\"bbbbbbbbbb\"]\n")
      expect(tempfile2.tap(&:rewind).read).to eq("[\"key3\",\"cccccccccc\"]\n[\"key4\",\"dddddddddd\"]\n")
    end

    it "sorts and reduces the chunks when writing them" do
      tempfile = Tempfile.new

      allow(Tempfile).to receive(:new).and_return(tempfile)

      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield("key3", 1)
        .and_yield("key1", 1)
        .and_yield("key2", 1)
        .and_yield("key1", 1)

      allow(implementation).to receive(:reduce) do |_key, count1, count2|
        count1 + count2
      end

      mapper = described_class.new(implementation, memory_limit: 35)
      mapper.map("key")

      expect(tempfile.tap(&:rewind).read).to eq("[\"key1\",2]\n[\"key2\",1]\n[\"key3\",1]\n")
    end
  end

  describe "#shuffle" do
    it "merges the chunks and yields the partitions with tempfiles" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield("key1", "a" * 10)
        .and_yield("key2", "b" * 10)
        .and_yield("key3", "c" * 10)
        .and_yield("key4", "d" * 10)
        .and_yield("key5", "e" * 10)

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(4), memory_limit: 50)
      mapper.map("key")

      result = mapper.shuffle.map { |partition, tempfile| [partition, tempfile.read] }

      expect(result).to eq(
        [
          [1, "[\"key1\",\"aaaaaaaaaa\"]\n[\"key4\",\"dddddddddd\"]\n"],
          [2, "[\"key2\",\"bbbbbbbbbb\"]\n[\"key3\",\"cccccccccc\"]\n"],
          [0, "[\"key5\",\"eeeeeeeeee\"]\n"]
        ]
      )
    end

    it "reduces each partition" do
      implementation = Object.new

      allow(implementation).to receive(:map)
        .and_yield("key3", 1)
        .and_yield("key1", 1)
        .and_yield("key2", 1)
        .and_yield("key1", 1)
        .and_yield("key3", 1)
        .and_yield("key1", 1)

      allow(implementation).to receive(:reduce) do |_, count1, count2|
        count1 + count2
      end

      mapper = described_class.new(implementation, partitioner: MapReduce::HashPartitioner.new(8), memory_limit: 20)
      mapper.map("key")

      result = mapper.shuffle.map { |partition, tempfile| [partition, tempfile.read] }

      expect(result).to eq(
        [
          [1, "[\"key1\",3]\n"],
          [2, "[\"key2\",1]\n"],
          [6, "[\"key3\",2]\n"]
        ]
      )
    end
  end
end
