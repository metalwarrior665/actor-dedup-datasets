## 2024-07-03
*Features*
- Enable merging all datasets for runs of an Actor or Task with `actorOrTaskId`, `onlyRunsNewerThan`, `onlyRunsOlderThan` input parameters.

## 2023-07-13
*Features*
- Add `customInputData` object to input for easy passing of custom values into `preDedupTransformFunction` and `postDedupTransformFunction`. It is part of the 2nd parameter object.

## 2021-01-24
*Featues*
- Added `fieldsToLoad` to input to increase speed and reducem meory if you don't need full items in output
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