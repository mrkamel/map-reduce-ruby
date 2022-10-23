class TestMergeable
  include MapReduce::Mergeable
end

RSpec.describe TestMergeable do
  describe "#k_way_merge" do
    it "yields all pairs in sort order of the key" do
      tempfile1 = Tempfile.new
      tempfile1.puts(JSON.generate(["key1", 1]))
      tempfile1.puts(JSON.generate(["key3", 2]))
      tempfile1.puts(JSON.generate(["key4", 3]))

      tempfile2 = Tempfile.new
      tempfile2.puts(JSON.generate(["key2", 1]))
      tempfile2.puts(JSON.generate(["key5", 2]))
      tempfile2.puts(JSON.generate(["key6", 3]))

      expect(described_class.new.send(:k_way_merge, [tempfile1.tap(&:rewind), tempfile2.tap(&:rewind)]).to_a).to eq(
        [
          ["key1", 1],
          ["key2", 1],
          ["key3", 2],
          ["key4", 3],
          ["key5", 2],
          ["key6", 3]
        ]
      )
    ensure
      tempfile1&.close(true)
      tempfile2&.close(true)
    end

    it "simply yields all pairs when only one file is given" do
      tempfile = Tempfile.new
      tempfile.puts(JSON.generate(["key1", 1]))
      tempfile.puts(JSON.generate(["key3", 2]))
      tempfile.puts(JSON.generate(["key4", 3]))

      expect(described_class.new.send(:k_way_merge, [tempfile.tap(&:rewind)]).to_a).to eq(
        [
          ["key1", 1],
          ["key3", 2],
          ["key4", 3]
        ]
      )
    ensure
      tempfile&.close(true)
    end
  end
end

