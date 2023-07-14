const Apify = require('apify');
const BigSet = require('big-set');

const { loadDatasetItemsInParallel } = require('./loader');
const { persistedPush, dedup } = require('./utils');

const { log } = Apify.utils;

module.exports = async ({
    datasetIds,
    batchSizeLoad,
    output,
    fields,
    parallelLoads,
    parallelPushes,
    outputDatasetId,
    uploadBatchSize,
    offset = 0,
    limit,
    fieldsToLoad,
    datasetIdsOfFilterItems,
    preDedupTransformFn,
    postDedupTransformFn,
    pushState,
    outputTo,
    migrationState,
    verboseLog,
    customInputData,
}) => {
    const dedupSet = new BigSet();

    if (datasetIdsOfFilterItems) {
        const items = await loadDatasetItemsInParallel(
            datasetIdsOfFilterItems,
            {
                batchSize: batchSizeLoad,
                // We only need to dedup fields here since we never push this
                fields,
                parallelLoads,
                debugLog: true,
                // For a single dataset, we can optimize the loading to skip loading what we pushed already after migration
                // TODO: Make this work for multiple datasets in loadDatasetItemsInParallel
            },
        );
        // This just fills the set
        dedup({ items, output: 'nothing', fields, dedupSet });
    }

    if (!pushState.pushedItemsCount) {
        pushState.pushedItemsCount = 0;
    }

    let items = await loadDatasetItemsInParallel(
        datasetIds,
        {
            batchSize: batchSizeLoad,
            parallelLoads,
            debugLog: true,
            // usually undefined
            fields: fieldsToLoad,
            // For a single dataset, we can optimize the loading to skip loading what we pushed already after migration
            // TODO: Fitx Optimize loading, e.g.  datasetIds.length === 1 ? offset + pushState.pushedItemsCount : offset,
            // is bugged because we load less items and then push less
            offset,
            limit,
        },
    );

    items = await preDedupTransformFn(items, { Apify, customInputData });

    let outputItems = dedup({ items, output, fields, dedupSet });

    outputItems = await postDedupTransformFn(outputItems, { Apify, customInputData });

    log.info(`Total loaded: ${items.length}, Total unique: ${dedupSet.size}, Total duplicates: ${items.length - dedupSet.size}`);

    log.info(`Going to push ${outputItems.length - pushState.pushedItemsCount} pending, ${outputItems.length} total`);

    const outputDataset = await Apify.openDataset(outputDatasetId);
    await persistedPush({ outputItems, parallelPushes, pushState, uploadBatchSize, output, outputDataset,
        outputTo, migrationState, verboseLog });
};
