# MapReduce

The MapReduce gem provides the easiest way to write custom, distributed, larger
than memory map-reduce jobs by using your local disk and some arbitrary storage
layer like s3. You can specify how much memory you are willing to offer and
MapReduce will use its buffers accordingly. Finally, you can use your already
existing background job system like `sidekiq` or one of its various
alternatives.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'map_reduce'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install map_reduce

## Usage

Any map-reduce job consists of an implementation of your `map` function, your
`reduce` function and worker code. So let's start with an implementation for
a word count map-reduce task which fetches txt documents from the web.

```ruby
class WordCounter
  def map(url)
    HTTP.get(url).to_s.split.each do |word|
      yield(word, 1)
    end
  end

  def reduce(word, count1, count2)
    count1 + count2
  end
end
```

Next, we need some worker code to run the mapping part:

```ruby
class WordCountMapper
  def perform(job_id, mapper_id, url)
    mapper = MapReduce::Mapper.new(WordCounter.new, num_partitions: 16, memory_limit: 100.megabytes)
    mapper.map(url)

    mapper.shuffle do |partition, tempfile|
      # store content of tempfile e.g. on s3:
      bucket.object("map_reduce/jobs/#{job_id}/partitions/#{partition}/chunk.#{mapper_id}.json").put(body: tempfile)
    end
  end
end
```

Finally, we need some worker code to run the reduce part:

```ruby
class WordCountReducer
  def perform(job_id, partition)
    reducer = MapReduce::Reducer.new(WordCounter.new)

    # fetch all chunks of the partitions e.g. from s3:
    bucket.list(prefix: "map_reduce/jobs/#{job_id}/partitions/#{partition}/").each do |object|
      tempfile = reducer.add_chunk

      object.download_file(tempfile.path)
    end

    reducer.reduce do |word, count|
      # each word with its final count
    end
  end
end
```

To run your mappers, you can do:

```ruby
job_id = SecureRandom.hex

list_of_urls.each_with_index do |url, index|
  WordCountMapper.perform_async(job_id, index, url)
end
```

And to run your reducers:

```ruby
(0..16).each do |partition|
  WordCountReducer.perform_async(job_id, partition)
end
```

How to run automate running the mappers and reducers in sequence, depends on
your background job system. The most simple approach is e.g. to track your
mapper state in redis and have a job to start your reducers which waits up
until your mappers are finished.

That's it.

## Internals

To fully understand the performance details, the following outlines the inner
workings of MapReduce. Of course, feel free to check the code as well.

`MapReduce::Mapper#map` calls your `map` implementation and adds each yielded
key-value pair to an internal buffer up until the memory limit is reached.
When the memory limit is reached, the buffer is sorted by key and fed through
your `reduce` implementation already, as this can greatly reduce the amount of
data already. The result is written to a tempfile. This proceeds up until all
key-value pairs are yielded. `MapReduce::Mapper#shuffle` then reads the first
key-value pair of all already sorted chunk tempfiles and adds them to a
priority queue using a binomial heap, such that with every `pop` operation on
that heap, we get items sorted by key. When the item returned by `pop` e.g.
belongs to the second chunk, then the next key-value pair of the second chunk
is subsequently read and added to the priority queue, up until no more pairs
are available. This guarantees that we sort all chunks without fully loading
them into memory and is called `k-way-merge`. With every `pop` operation, your
`reduce` implementation is continously called up until the key changes between
two calls to `pop`. When the key changes, the key is known to be fully reduced,
such that the key is hashed modulo the number of partitions and gets written to
the correct partition tempfile.

The resulting partition tempfiles need to be stored in some global storage
system like s3, such that your workers can mappers can upload them and the
reducers can download them.

`MapReduce::Reducer#add_chunk` adds and registers a new tempfile such that your
reducer can download a mapper file for the particular partition and write its
contents to that tempfile. `MapReduce::Reducer#reduce` finally again builds up
a priority queue and performs `k-way-merge`, feeds the key-value pairs into
your reduce implementation up until a key change between `pop` operations occur
and yields the fully reduced key-value pair.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run
`rake spec` to run the tests. You can also run `bin/console` for an interactive
prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To
release a new version, update the version number in `version.rb`, and then run
`bundle exec rake release`, which will create a git tag for the version, push
git commits and the created tag, and push the `.gem` file to
[rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/mrkamel/map_reduce.

## License

The gem is available as open source under the terms of the [MIT
License](https://opensource.org/licenses/MIT).
