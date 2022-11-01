class TestMergeable
  include MapReduce::Mergeable
end

module MapReduce
  RSpec.describe TestMergeable do
    describe "#k_way_merge" do
      it "yields all pairs in sort order of the key" do
        temp_path1 = TempPath.new
        file1 = File.open(temp_path1.path, "w+")
        file1.puts(JSON.generate(["key1", 1]))
        file1.puts(JSON.generate(["key3", 2]))
        file1.puts(JSON.generate(["key4", 3]))
        file1.close

        temp_path2 = TempPath.new
        file2 = File.open(temp_path2.path, "w+")
        file2.puts(JSON.generate(["key2", 1]))
        file2.puts(JSON.generate(["key5", 2]))
        file2.puts(JSON.generate(["key6", 3]))
        file2.close

        temp_path3 = TempPath.new
        file3 = File.open(temp_path3.path, "w+")
        file3.puts(JSON.generate(["key3", 1]))
        file3.puts(JSON.generate(["key7", 2]))
        file3.puts(JSON.generate(["key8", 3]))
        file3.close

        expect(described_class.new.send(:k_way_merge, [temp_path1, temp_path2, temp_path3], chunk_limit: 2).to_a).to eq(
          [
            ["key1", 1],
            ["key2", 1],
            ["key3", 1],
            ["key3", 2],
            ["key4", 3],
            ["key5", 2],
            ["key6", 3],
            ["key7", 2],
            ["key8", 3]
          ]
        )
      end

      it "simply yields all pairs when only one file is given" do
        temp_path = TempPath.new
        file = File.open(temp_path.path, "w+")
        file.puts(JSON.generate(["key1", 1]))
        file.puts(JSON.generate(["key3", 2]))
        file.puts(JSON.generate(["key4", 3]))
        file.close

        expect(described_class.new.send(:k_way_merge, [temp_path], chunk_limit: 2).to_a).to eq(
          [
            ["key1", 1],
            ["key3", 2],
            ["key4", 3]
          ]
        )
      end
    end
  end
end
