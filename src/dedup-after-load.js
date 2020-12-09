const Apify = require('apify');
const BigMap = require('big-map-simple');

const { loadDatasetItemsInParallel } = require('./loader');

const { log } = Apify.utils;

module.exports = async ({ datasetIds, batchSizeLoad, output, fields, parallelLoads, outputDatasetId, uploadBatchSize, uploadSleepMs }) => {
    let pushedItemsCount = (await Apify.getValue('PUSHED')) || 0;

    const items = await loadDatasetItemsInParallel(
        datasetIds,
        {
            batchSize: batchSizeLoad,
            fields,
            parallelLoads,
            debugLog: true,
            // For a single dataset, we can optimize the loading to skip loading what we pushed already after migration
            // TODO: Make this work for multiple datasets in loadDatasetItemsInParallel
            offset: datasetIds.length === 1 ? pushedItemsCount : 0,
        },
    );

    // We dedup everything at once
    const dedupStart = Date.now();
    const dedupMap = new BigMap();

    for (const item of items) {
        const key = fields.map((field) => item[field]).join('');
        // If we want to push duplicates, we have to do a little trickery
        if (output === 'duplicate-items') {
            const enhancedItem = { duplicationKey: key, ...item };
            if (!dedupMap.has(key)) {
                dedupMap.set(key, [enhancedItem]);
            } else {
                const duplicates = dedupMap.get(key).concat(enhancedItem);
                dedupMap.set(key, duplicates);
            }
        } else {
            // Normal path
            // eslint-disable-next-line no-lonely-if
            if (!dedupMap.has(key)) {
                dedupMap.set(key, item);
            }
        }
    }

    log.info(`Dedup took ${Math.round((Date.now() - dedupStart) / 1000)} seconds`);

    const valuesStart = Date.now();

    // If outputDuplicates === true, each item is an array, 1 length if unique, more if duplicates
    // Else it is a normal array of items
    const itemsWithDupsAsArrays = dedupMap.values();
    log.info(`Turning Map into values took ${Math.round((Date.now() - valuesStart) / 1000)} seconds`);
    log.info(`Total loaded: ${items.length}, Total unique: ${itemsWithDupsAsArrays.length}, Total duplicates: ${items.length - itemsWithDupsAsArrays.length}`);

    const outputDataset = await Apify.openDataset(outputDatasetId);

    let isMigrating = false;
    Apify.events.on('migrating', () => {
        isMigrating = true;
    });

    let outputItems = itemsWithDupsAsArrays;
    if (output === 'duplicate-items') {
        outputItems = itemsWithDupsAsArrays
            .filter((item) => item.length > 1)
            .flatMap((item) => item.slice(1)); // We skip the first item which is unique
    }

    log.info(`Going to push ${outputItems.length - pushedItemsCount} pending, ${outputItems.length} total`);

    // Now we push from the whole BigMap
    if (output !== 'nothing') {
        // We start pushing where we left the state (if migrated)
        // Everything is always in the same order so we can use just a single index
        for (let i = pushedItemsCount; i < outputItems.length; i += uploadBatchSize) {
            if (isMigrating) {
                log.warning('Forever sleeping until migration');
                // Do nothing
                await new Promise(() => {});
            }
            const itemsToPush = outputItems.slice(i, i + uploadBatchSize);

            await outputDataset.pushData(itemsToPush);
            pushedItemsCount += itemsToPush.length;
            await Apify.setValue('PUSHED', pushedItemsCount);
            const { itemCount } = await outputDataset.getInfo();
            log.info(`Pushed total: ${pushedItemsCount}, In dataset (delayed): ${itemCount}`);
            await Apify.utils.sleep(uploadSleepMs);
        }
    }
};
