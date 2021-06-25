RSpec.describe MapReduce::Reducer do
  describe "#add_chunk" do
    it "creates and returns a tempfile" do
      allow(Tempfile).to receive(:new).and_return("tempfile")

      reducer = described_class.new(nil)

      expect(reducer.add_chunk).to eq("tempfile")
    end
  end

  describe "#reduce" do
    it "merges the sorted chunks and yields the pairs" do
      implementation = Object.new

      allow(implementation).to receive(:reduce) do |_, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      reducer = described_class.new(implementation)

      chunk1 = reducer.add_chunk
      chunk1.puts(JSON.generate([{ "key" => "key1" }, { "value" => 1 }]))
      chunk1.puts(JSON.generate([{ "key" => "key2" }, { "value" => 1 }]))

      chunk2 = reducer.add_chunk
      chunk2.puts(JSON.generate([{ "key" => "key3" }, { "value" => 1 }]))
      chunk2.puts(JSON.generate([{ "key" => "key4" }, { "value" => 1 }]))

      expect(reducer.reduce.to_a).to eq(
        [
          [{ "key" => "key1" }, { "value" => 1 }],
          [{ "key" => "key2" }, { "value" => 1 }],
          [{ "key" => "key3" }, { "value" => 1 }],
          [{ "key" => "key4" }, { "value" => 1 }]
        ]
      )
    end

    it "reduces the sorted chunks" do
      implementation = Object.new

      allow(implementation).to receive(:reduce) do |_, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      reducer = described_class.new(implementation)

      chunk1 = reducer.add_chunk
      chunk1.puts(JSON.generate([{ key: "key1" }, { value: 1 }]))
      chunk1.puts(JSON.generate([{ key: "key2" }, { value: 1 }]))
      chunk1.puts(JSON.generate([{ key: "key3" }, { value: 1 }]))

      chunk2 = reducer.add_chunk
      chunk2.puts(JSON.generate([{ key: "key2" }, { value: 1 }]))
      chunk2.puts(JSON.generate([{ key: "key3" }, { value: 1 }]))

      chunk3 = reducer.add_chunk
      chunk3.puts(JSON.generate([{ key: "key3" }, { value: 1 }]))
      chunk3.puts(JSON.generate([{ key: "key4" }, { value: 1 }]))

      expect(reducer.reduce.to_a).to eq(
        [
          [{ "key" => "key1" }, { "value" => 1 }],
          [{ "key" => "key2" }, { "value" => 2 }],
          [{ "key" => "key3" }, { "value" => 3 }],
          [{ "key" => "key4" }, { "value" => 1 }]
        ]
      )
    end
  end
end
