const Apify = require('apify');

const { log } = Apify.utils;

module.exports.getRealDatasetId = async (datasetIdOrName) => {
    const client = Apify.newClient();
    const couldBeId = datasetIdOrName.match(/[a-zA-Z0-9]{17}/);
    let datasetInfo;
    if (couldBeId) {
        try {
            datasetInfo = await client.dataset(datasetIdOrName).get();
        } catch (e) {}
    }
    if (!datasetInfo) {
        datasetInfo = await client.datasets().getOrCreate(datasetIdOrName);
        log.info(`Output dataset ID was not provided, `
            + `will use or create named dataset: ${datasetIdOrName} with ID: ${datasetInfo.id}`);
    }
    return datasetInfo.id;
};

// We don't care about clearing it here
module.exports.betterSetInterval = (func, delay) => {
    const funcWrapper = function () {
        func(callback);
    };
    const callback = function () {
        setTimeout(funcWrapper, delay);
    };
    funcWrapper();
};

module.exports.dedup = ({ items, output, fields, dedupSet }) => {
    // If no fields are provided, we don't do any deduping
    if (!fields || fields.length === 0) {
        return items;
    }
    // const dedupStart = Date.now();

    const outputItems = [];
    for (const item of items) {
        const key = fields
            .map((field) => {
                const value = item[field];
                // For deep equal comparing
                return typeof value === 'object' ? JSON.stringify(value) : value;
            })
            .join('');
        const hasKey = dedupSet.has(key);
        if (output === 'unique-items') {
            if (!hasKey) {
                outputItems.push(item);
            }
        } else if (output === 'duplicate-items') {
            if (hasKey) {
                const enhancedItem = { duplicationKey: key, ...item };
                outputItems.push(enhancedItem);
            }
        }
        if (!hasKey) {
            dedupSet.add(key);
        }
    }

    // log.info(`Dedup took ${Math.round((Date.now() - dedupStart) / 1000)} seconds`);
    return outputItems;
};

module.exports.persistedPush = async ({
    outputItems,
    parallelPushes,
    uploadBatchSize,
    outputDataset,
    output,
    uploadSleepMs,
    pushState,
    datasetId,
    datasetOffset,
    // Only for KVs
    outputTo,
    migrationState,
    verboseLog,
}) => {
    // NOTE: For dedup-as-loading, uploadBatchSize must be always bigger or equal to outputItems
    // In the crawler, we do this by setting the download batch == upload batch
    // This must be true because we persist deduping for the whole dedup batch so
    // once we do that, we must ensure it is pushed

    // Or it is push as loading
    const isPushAfterLoad = typeof pushState.pushedItemsCount === 'number';

    // Now we push from the whole BigMap
    if (output !== 'nothing') {
        // We start pushing where we left the state (if migrated)
        // Everything is always in the same order so we can use just a single index

        const pushedItemsCount = isPushAfterLoad ? pushState.pushedItemsCount : pushState[datasetId][datasetOffset];

        if (!isPushAfterLoad && verboseLog) {
            log.info(`[Batch-${datasetId}-${datasetOffset}]: `
                + `Starting to push: ${pushedItemsCount}/${outputItems.length} was already pushed before restarting`);
        }
        for (let i = pushedItemsCount; i < outputItems.length; i += uploadBatchSize) {
            // dedup-as-loading has to be paused before dedup occurs, not here
            if (isPushAfterLoad && migrationState.isMigrating) {
                log.warning('Actor migration is in process, no more data will be pushed in this batch');
                // Do nothing
                await new Promise(() => {});
            }
            const itemsToPush = outputItems.slice(i, i + uploadBatchSize);

            const updatePushState = () => {
                if (!isPushAfterLoad) {
                    // Means dedup as loading
                    pushState[datasetId][datasetOffset] = i + itemsToPush.length;
                } else {
                    pushState.pushedItemsCount = i + itemsToPush.length;
                }
            };

            if (outputTo === 'dataset') {
                // We enable doing more pushes in parallel inside a single batch
                // This doesn't affect the state at all, only speeds up the batch upload
                const pushPromises = [];
                const parallelizedBatchSize = Math.ceil(itemsToPush.length / parallelPushes);
                for (let j = 0; j < parallelPushes; j++) {
                    const start = j * parallelizedBatchSize;
                    const end = (j + 1) * parallelizedBatchSize;
                    const parallelPushChunk = itemsToPush.slice(start, end);
                    pushPromises.push(outputDataset.pushData(parallelPushChunk));
                }
                // We must update it before this await because the push can take time
                // and migration can cut us off
                updatePushState();
                await Promise.all(pushPromises);
            } else if (outputTo === 'key-value-store') {
                const iterationIndex = Math.floor(i / uploadBatchSize);
                const datasetIdString = datasetId ? `-${datasetId}` : '';
                const datasetOffsetString = datasetOffset ? `-${datasetOffset}` : '';
                const recordKey = `OUTPUT${datasetIdString}${datasetOffsetString}-${iterationIndex}`;
                updatePushState();
                await Apify.setValue(recordKey, itemsToPush);
            }

            if (isPushAfterLoad) {
                log.info(`Pushed total: ${i + itemsToPush.length}`);
            } else {
                if (verboseLog) {
                    log.info(`[Batch-${datasetId}-${datasetOffset}]: `
                        + `Pushed in batch: ${i + itemsToPush.length}/${outputItems.length}`);
                }
            }
            await Apify.utils.sleep(uploadSleepMs);
        }
    }
};
