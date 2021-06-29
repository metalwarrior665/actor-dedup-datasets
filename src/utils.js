const Apify = require('apify');

const { log } = Apify.utils;

module.exports.dedup = ({ items, output, fields, dedupSet }) => {
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
}) => {
    let isMigrating = false;
    Apify.events.on('migrating', () => {
        isMigrating = true;
    });
    Apify.events.on('aborting', () => {
        isMigrating = true;
    });

    // Or it is push as loading
    const isPushAfterLoad = typeof pushState.pushedItemsCount === 'number';

    // Now we push from the whole BigMap
    if (output !== 'nothing') {
        // We start pushing where we left the state (if migrated)
        // Everything is always in the same order so we can use just a single index

        const pushedItemsCount = isPushAfterLoad ? pushState.pushedItemsCount : pushState[datasetId][datasetOffset];
        for (let i = pushedItemsCount; i < outputItems.length; i += uploadBatchSize) {
            if (isMigrating) {
                log.warning('Forever sleeping until migration');
                // Do nothing
                await new Promise(() => {});
            }
            const itemsToPush = outputItems.slice(i, i + uploadBatchSize);

            if (outputTo === 'dataset') {
                // We enable doing more pushes in parallel inside a single batch
                // This doesn't affect the state at all, only speeds up the batch upload
                const pushPromises = [];
                const parallelizedBatchSize = Math.ceil(itemsToPush.length / parallelPushes);
                for (let i = 0; i < parallelPushes; i++) {
                    const start = i * parallelizedBatchSize;
                    const end = (i + 1) * parallelizedBatchSize;
                    const parallelPushChunk = itemsToPush.slice(start, end);
                    pushPromises.push(outputDataset.pushData(parallelPushChunk));
                }
                await Promise.all(pushPromises);
            } else if (outputTo === 'key-value-store') {
                const iterationIndex = Math.floor(i / uploadBatchSize);
                const datasetIdString = datasetId ? `-${datasetId}` : '';
                const datasetOffsetString = datasetOffset ? `-${datasetOffset}` : '';
                const recordKey = `OUTPUT${datasetIdString}${datasetOffsetString}-${iterationIndex}`;
                await Apify.setValue(recordKey, itemsToPush);
            }

            if (!isPushAfterLoad) {
                // Means dedup as loading
                pushState[datasetId][datasetOffset] = i + itemsToPush.length;
            } else {
                pushState.pushedItemsCount = i + itemsToPush.length;
            }

            const itemCount = outputTo === 'dataset' ? (await outputDataset.getInfo().then((res) => res.itemCount)) : 'output in KV';
            if (isPushAfterLoad) {
                log.info(`Pushed total: ${i + itemsToPush.length}, In dataset (delayed): ${itemCount}`);
            } else {
                log.info(`[Batch-${datasetId}-${datasetOffset}]: Pushed total: ${i + itemsToPush.length}, In dataset (delayed): ${itemCount}`);
            }
            await Apify.utils.sleep(uploadSleepMs);
        }
    }
};
