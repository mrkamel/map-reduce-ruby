RSpec.describe MapReduce::Reducer do
  describe "#add_chunk" do
    it "creates and returns a tempfile" do
      temp_path = instance_double(MapReduce::TempPath)
      allow(temp_path).to receive(:path).and_return("/path/to/file")
      allow(MapReduce::TempPath).to receive(:new).and_return(temp_path)

      reducer = described_class.new(nil)

      expect(reducer.add_chunk).to eq("/path/to/file")
    end
  end

  describe "#reduce" do
    it "merges the sorted chunks and yields the pairs" do
      implementation = Object.new

      allow(implementation).to receive(:reduce) do |_, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      reducer = described_class.new(implementation)

      File.open(reducer.add_chunk, "w") do |file|
        file.puts(JSON.generate([{ "key" => "key1" }, { "value" => 1 }]))
        file.puts(JSON.generate([{ "key" => "key2" }, { "value" => 1 }]))
      end

      File.open(reducer.add_chunk, "w") do |file|
        file.puts(JSON.generate([{ "key" => "key3" }, { "value" => 1 }]))
        file.puts(JSON.generate([{ "key" => "key4" }, { "value" => 1 }]))
      end

      expect(reducer.reduce(chunk_limit: 32).to_a).to eq(
        [
          [{ "key" => "key1" }, { "value" => 1 }],
          [{ "key" => "key2" }, { "value" => 1 }],
          [{ "key" => "key3" }, { "value" => 1 }],
          [{ "key" => "key4" }, { "value" => 1 }]
        ]
      )
    end

    it "reduces the sorted chunks and deletes chunk files" do
      implementation = Object.new

      allow(implementation).to receive(:reduce) do |_, count1, count2|
        { "value" => count1["value"] + count2["value"] }
      end

      reducer = described_class.new(implementation)

      paths = Array.new(3) { reducer.add_chunk }

      File.open(paths[0], "w") do |file|
        file.puts(JSON.generate([{ key: "key1" }, { value: 1 }]))
        file.puts(JSON.generate([{ key: "key2" }, { value: 1 }]))
        file.puts(JSON.generate([{ key: "key3" }, { value: 1 }]))
      end

      File.open(paths[1], "w") do |file|
        file.puts(JSON.generate([{ key: "key2" }, { value: 1 }]))
        file.puts(JSON.generate([{ key: "key3" }, { value: 1 }]))
      end

      File.open(paths[2], "w") do |file|
        file.puts(JSON.generate([{ key: "key3" }, { value: 1 }]))
        file.puts(JSON.generate([{ key: "key4" }, { value: 1 }]))
      end

      expect(reducer.reduce(chunk_limit: 2).to_a).to eq(
        [
          [{ "key" => "key1" }, { "value" => 1 }],
          [{ "key" => "key2" }, { "value" => 2 }],
          [{ "key" => "key3" }, { "value" => 3 }],
          [{ "key" => "key4" }, { "value" => 1 }]
        ]
      )

      paths.each do |path|
        expect(File.exist?(path)).to eq(false)
      end
    end

    it "does not yield when there is nothing to reduce" do
      expect(described_class.new(Object.new).reduce(chunk_limit: 32).to_a).to eq([])
    end

    it "raises a InvalidChunkLimit error when chunk_limit is less than 2" do
      expect { described_class.new(Object.new).reduce(chunk_limit: 1).to_a }
        .to raise_error(described_class::InvalidChunkLimit)
    end
  end
end
