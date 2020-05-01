# Dedup datasets

Deduplicates one or more datasets by a set of fields and merges them together into one dataset of unique items.

## Input
- `datasetIds` \<Array\<String\>\> Datasets that should be deduplicated and merged. **Required**
- `fields` \<Array\<String\>\> Fields whose combination should be unique for the item to be considered unique. **Required**
- `doPush` \<Boolean\> If true, will also push unique items into new dataset. False is useful for getting information about number of duplicates. **Default:** `true`
- `outputDatasetId` \<String\> Optionally can push into non-default dataset.
- `uploadSleepMs` \<Number\> How long it should wait between each batch when uploading. Useful to not overload Apify API. **Default**: `5000`
- `uploadBatchSize` \<Number\> How many items it should upload in one pushData call. Useful to not overload Apify API. **Default**: `2000`
