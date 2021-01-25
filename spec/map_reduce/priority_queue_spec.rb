RSpec.describe MapReduce::PriorityQueue do
  let(:priority_queue) { described_class.new }

  describe "#push" do
    it "adds the item to the queue" do
      priority_queue.push("item", "item")

      expect(priority_queue.pop).to eq("item")
    end

    it "allows ad add complex objects" do
      priority_queue.push(%w[some item], "item")

      expect(priority_queue.pop).to eq(%w[some item])
    end

    it "allows to add the same item twice" do
      priority_queue.push("item", "item")
      priority_queue.push("item", "item")

      expect(priority_queue.pop).to eq("item")
      expect(priority_queue.pop).to eq("item")
    end
  end

  describe "#pop" do
    it "returns items sorted by the key" do
      priority_queue.push("item1", "key3")
      priority_queue.push("item2", "key1")
      priority_queue.push("item3", "key2")

      expect(priority_queue.pop).to eq("item2")
      expect(priority_queue.pop).to eq("item3")
      expect(priority_queue.pop).to eq("item1")
    end
  end
end
