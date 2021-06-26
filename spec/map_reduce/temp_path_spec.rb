RSpec.describe MapReduce::TempPath do
  describe "#path" do
    it "uses Dir::Tmpname to generate a temporary path" do
      allow(Dir::Tmpname).to receive(:create).and_yield.and_return("/path/to/file")

      expect(described_class.new.path).to eq("/path/to/file")
    end

    it "returns a usable path" do
      temp_path = described_class.new

      File.write(temp_path.path, "text")
    end
  end

  describe "#delete" do
    it "deletes the file" do
      temp_path = described_class.new
      File.write(temp_path.path, "text")
      temp_path.delete

      expect(File.exist?(temp_path.path)).to eq(false)
    end

    it "does not do anything when the file does not exist" do
      expect { described_class.new.delete }.not_to raise_error
    end
  end

  describe ".finalize" do
    it "returns a proc which deletes the specified path" do
      temp_path = described_class.new
      File.write(temp_path.path, "text")
      described_class.finalize(temp_path.path).call

      expect(File.exist?(temp_path.path)).to eq(false)
    end

    it "does not do anything when the file does not exist" do
      temp_path = described_class.new
      expect { described_class.finalize(temp_path.path).call }.not_to raise_error
    end
  end
end
