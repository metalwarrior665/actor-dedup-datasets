const Apify = require('apify');

const { log } = Apify.utils;

// dedupSet is only present as param when called from dedup-as-loading
module.exports.dedup = ({ items, output, fields, dedupSet }) => {
    const dedupStart = Date.now();

    const outputItems = [];
    for (const item of items) {
        const key = fields.map((field) => item[field]).join('');
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
    uploadBatchSize,
    outputDataset,
    output,
    uploadSleepMs,
    pushState,
    // These 2 fields are for dedup-as-loading
    datasetId,
    datasetOffset,
}) => {
    let isMigrating = false;
    Apify.events.on('migrating', () => {
        isMigrating = true;
    });

    // Now we push from the whole BigMap
    if (output !== 'nothing') {
        // We start pushing where we left the state (if migrated)
        // Everything is always in the same order so we can use just a single index
        const pushedItemsCount = datasetId ? pushState[datasetId][datasetOffset] : pushState.pushedItemsCount;
        for (let i = pushedItemsCount; i < outputItems.length; i += uploadBatchSize) {
            if (isMigrating) {
                log.warning('Forever sleeping until migration');
                // Do nothing
                await new Promise(() => {});
            }
            const itemsToPush = outputItems.slice(i, i + uploadBatchSize);

            await outputDataset.pushData(itemsToPush);
            if (datasetId) {
                // Means dedup as loading
                pushState[datasetId][datasetOffset] = i + itemsToPush.length;
            } else {
                pushState.pushedItemsCount = i + itemsToPush.length;
            }
            const { itemCount } = await outputDataset.getInfo();
            if (!datasetId) {
                log.info(`Pushed total: ${i + itemsToPush.length}, In dataset (delayed): ${itemCount}`);
            } else {
                log.info(`[Batch-${datasetId}-${datasetOffset}]: Pushed total: ${i + itemsToPush.length}, In dataset (delayed): ${itemCount}`);
            }
            await Apify.utils.sleep(uploadSleepMs);
        }
    }
};
