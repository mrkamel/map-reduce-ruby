RSpec.describe MapReduce::HashPartitioner do
  describe "#call" do
    it "uses sha1 of the json representation of the key to calculate a hash" do
      allow(Digest::SHA1).to receive(:hexdigest).and_return("fff...")

      described_class.new(8).call(some: "key")

      expect(Digest::SHA1).to have_received(:hexdigest).with('{"some":"key"}')
    end

    it "uses the first five letters of the hash modulo the number of partitions to calculate the partition" do
      allow(Digest::SHA1).to receive(:hexdigest).and_return("fffffffff")

      partition = described_class.new(4).call("key")

      expect(partition).to eq("fffff".to_i(16) % 4)
      expect(partition).to eq(3)

      partition = described_class.new(8).call("key")

      expect(partition).to eq("fffff".to_i(16) % 8)
      expect(partition).to eq(7)
    end
  end
end
