# The ultimate dataset processing actor - merge, dedup & transform

Refined and optimized dataset processing actor for large scale merging, deduplications and transformation

## Why to use this actor
- Extremely fast data processing thanks for parallelizing workloads (easily 20x faster than default loading/pushing datasets)
- Allows reading from multiple datasets silmutanesously, ideal for merging after scraping with many runs
- Actor migration proof - All steps that can be persisted are persisted => work is not repeated and no duplicated data pushed
- "Dedup as loading" mode allows for near constant memory processing even for huge datasets (think 10M+)
- Deduplication allows for combination of many fields and even nested objects/arrays (those are JSON.stringified for deep equality check)
- Allows for storing into KV store records
- Allows super fast blank runs that count duplicates

## Input
Detailed INPUT table with description can be found on the [actor's public page](https://apify.com/lukaskrivka/dedup-datasets/input-schema).

