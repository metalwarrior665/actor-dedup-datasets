## 2024-09-25
*Features*
- Add `appendDatasetIds` to input. This is useful for transforming functions if you need to check which dataset each item comes from.
- Add `diff` function to transform function's second parameter object. This is using the [fast-diff](https://www.npmjs.com/package/fast-diff) package. This can be used to compare changes between string fields of two datasets.

## 2024-09-10
*Features*
- Add `persistedSharedObject` to transform functions input object. This object is shared for all transform function calls and persist over Actor migrations. This is useful mainly for the `dedup-as-loading` mode where the transform functions are called multiple times and only process a chunk of the data.
- Add `nullAsUnique` to input. If set to true, the `null` and missing values are considered unique and not deduplicated.

## 2024-07-03
*Features*
- Enable merging all datasets for runs of an Actor or Task with `actorOrTaskId`, `onlyRunsNewerThan`, `onlyRunsOlderThan` input parameters.

## 2023-07-13
*Features*
- Add `customInputData` object to input for easy passing of custom values into `preDedupTransformFunction` and `postDedupTransformFunction`. It is part of the 2nd parameter object.

## 2021-01-24
*Features*
- Added `fieldsToLoad` to input to increase speed and reduce memory if you don't need full items in output
- Added `limit` and `offset` to input to be able to process only slices of dataset
- Removed `uploadSleepMs` as the platform can now handle much higher load of upload

## 2021-01-14
*Features*
- `outputDatasetId` can now also use dataset name. If dataset with that name doesn't exist, a new dataset is created.

## 2020-07-10
*Fixes:*
- `dedup-as-loading` mode now works correctly with actor migrations. This means that this actor can finally be used for huge datasets with lower memory!

*Features:*
- `fields` are now optional which means the actor does not need to perform deduplication

## Previous updates
Previous updates were not tracked, see GitHub commits if you need to find past changes or ask in Issues or Discord.