# CHANGELOG

## v3.0.0

* [BREAKING] `MapReduce::Mapper#shuffle` now yields a hash of (partition, path)
  pairs, which e.g. allows to upload the files in parallel
* [BREAKING] `MapReduce::Mapper#shuffle` now requires a `chunk_limit`. This
  allows to further limit the maximum number of open file descriptors
* [BREAKING] `MapReduce::Reducer::InvalidChunkLimit` is now
  `MapReduce::InvalidChunkLimit` and inherits from `MapReduce::Error` being the
  base class for all errors
* `MapReduce::Mapper#shuffle` no longer keeps all partition files open. Instead,
  it writes them one after another to further strictly reduce the number of
  open file descriptors.

## v2.1.1

* Fix in `MapReduce::Mapper` when no `reduce` implementation is given

## v2.1.0

* Do not reduce in `MapReduce::Mapper` when no `reduce` implementation is given

## v2.0.0

* [BREAKING] Keys are no longer automatically converted to json before using
  them for sorting
  * This allows to have proper semantic sort order for numeric keys in addition
    to just the clustering of keys
  * Examples of valid keys: `"key"`, `["foo", 1.0]`, `["foo", ["bar"]]`
  * Examples of problematic keys: `nil`, `true`, `["foo", nil]`, `{ "foo" => "bar" }`
  * For migration purposes it is recommended to convert your keys to and from
    json manually if you have complex keys using `JSON.generate`/`JSON.parse`:

```ruby
class WordCounter
  def map(url)
    HTTP.get(url).to_s.split.each do |word|
      yield(JSON.generate("key" => word), 1) # if you use a hash for the key
    end
  end

  def reduce(json_key, count1, count2)
    key = JSON.parse(json_key) # if you want to access the original key

    count1 + count2
  end
end
```
