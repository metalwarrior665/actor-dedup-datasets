const Apify = require('apify');
const BigSet = require('big-set');

const { loadDatasetItemsInParallel } = require('./loader');
const { persistedPush, dedup } = require('./utils');

const { log } = Apify.utils;

// Here we dedup&push as we load the batches.
// Note that high concurrency can overload the dataset and introduce duplicates (due to bug in dataset backend)
module.exports = async ({
    datasetIds,
    batchSizeLoad,
    output,
    fields,
    parallelLoads,
    outputDatasetId,
    uploadBatchSize,
    uploadSleepMs,
    datasetIdsOfFilterItems,
    preDedupTransformFn,
    postDedupTransformFn,
    pushState,
    outputTo,
}) => {
    // We fill the state with datasetId object in the start to keep track of each dataset/offset batches
    for (const datasetId of datasetIds) {
        if (!pushState[datasetId]) {
            pushState[datasetId] = {};
        }
    }

    const outputDataset = await Apify.openDataset(outputDatasetId);

    const dedupSet = new BigSet();

    // We call this on every new batch of items
    const processFn = async (items, { datasetId, datasetOffset }) => {
        items = preDedupTransformFn(items);
        // We always process the whole batch but we push only those that were not pushed
        // The order inside a single batch is stable so we can do that
        let outputItems = dedup({ items, output, fields, dedupSet });
        log.info(`[Batch-${datasetId}-${datasetOffset}]: Loaded: ${items.length}, Total unique: ${dedupSet.size}`);

        outputItems = postDedupTransformFn(outputItems);

        if (typeof pushState[datasetId][datasetOffset] !== 'number') {
            pushState[datasetId][datasetOffset] = 0;
        }

        const pushConfig = {
            pushState,
            datasetId,
            datasetOffset,
        };

        await persistedPush({ outputItems, uploadBatchSize, output, outputDataset, uploadSleepMs, ...pushConfig, outputTo });
    };

    const processFnNoPush = async (items, { datasetId, datasetOffset }) => {
        items = preDedupTransformFn(items);
        dedup({ items, output: 'nothing', fields, dedupSet });
        log.info(`[Batch-${datasetId}-${datasetOffset}]: Loaded: ${items.length}, Total unique: ${dedupSet.size}`);
    };

    if (datasetIdsOfFilterItems) {
        await loadDatasetItemsInParallel(
            datasetIdsOfFilterItems,
            {
                fields,
                batchSize: batchSizeLoad,
                parallelLoads,
                debugLog: true,
                processFn: processFnNoPush,
            },
        );
    }

    await loadDatasetItemsInParallel(
        datasetIds,
        {
            batchSize: batchSizeLoad,
            parallelLoads,
            debugLog: true,
            processFn,
            // TODO: Implement smart Set persistence, probably split every 100k of Set keys into single KV record
            // persistLoadingStateForProcesFn: true,
        },
    );
};
