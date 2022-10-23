# CHANGELOG

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
