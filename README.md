# The ultimate dataset processing actor - merge, dedup & transform

Refined and optimized dataset processing actor for large scale merging, deduplications and transformation

## Why to use this actor
- Extremely fast data processing thanks for parallelizing workloads (easily 20x faster than default loading/pushing datasets)
- Allows reading from multiple datasets silmutanesously, ideal for merging after scraping with many runs
- Actor migration proof - All steps that can be persisted are persisted => work is not repeated and no duplicated data pushed
- `Dedup as loading` mode allows for near constant memory processing even for huge datasets (think 10M+)
- Deduplication allows for combination of many fields and even nested objects/arrays (those are JSON.stringified for deep equality check)
- Allows for storing into KV store records
- Allows super fast blank runs that count duplicates

## Merging
You can provide more than one dataset. In that case all items are merged into single dataset or key value store output. If you use the `Dedup after load` mode, the order of items will retain the order of datasets provided.

## Deduplication
If you optionally provide deduplication `fields`, this actor will deduplicate the dataset items. The deduplication process check the values of each field for equality and only return the first unique one (the first item that has a unique value for that field).

You can provide more than one field. In that case a combined string of that fields is checked, e.g. `"name": "Adidas Shoes, "id": "12345"` gets converted into `"Adidas Shoes12345"` for the checking purpose. So only items that have both fields the same are considered duplicates. This means the more fields you add, the less duplicates will be found.

Fields that are objects or arrays are also deeply compared via `JSON.stringify`. Just be aware that doing this for very large structures might have performance implications.

## Transformation
This actor enables you to do arbitrary data transformations before and after deduplication via `preDedupTransformFunction` and `postDedupTransformFunction`.

These functions simply take the array of items and should return array of items. You don't need to necessarily return the same amount of items (can filter some out or add new ones).

You can access an object with helper variables, currently containing the [Apify SDK reference](https://sdk.apify.com/docs/api/apify)

The default transformation does nothing with the items:
```javascript
(items, { Apify }) => {
    return items;
}
```

In case of `dedup-as-loading` mode, you only have access to the items of the specific batch.
But you can also access `datasetId` and `datasetOffset` parameters as each batch is only from one dataset.
```javascript
(items, { Apify, datasetId, datasetOffset }) => {
    return items;
}
```

## Input
Detailed INPUT table with description can be found on the [actor's public page](https://apify.com/lukaskrivka/dedup-datasets/input-schema).

## Changelog
Check the list of past updates [here](https://github.com/metalwarrior665/actor-dedup-datasets/blob/master/CHANGELOG.md)

