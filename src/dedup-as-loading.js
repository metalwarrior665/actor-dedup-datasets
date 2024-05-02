const Apify = require('apify');

const { loadDatasetItemsInParallel } = require('./loader');
const { persistedPush, dedup } = require('./utils');
const { createParallelPersistedSet } = require('./persisted-set');

const { log } = Apify.utils;

// Here we dedup&push as we load the batches.
// Note that high concurrency can overload the dataset and introduce duplicates (due to bug in dataset backend)
module.exports = async ({
    datasetIds,
    batchSizeLoad,
    output,
    fields,
    parallelLoads,
    parallelPushes,
    outputDatasetId,
    uploadBatchSize,
    offset,
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
    const outputDataset = await Apify.openDataset(outputDatasetId);

    // This ensures we keep unique dedup set for each actor run
    const fixedDate = (await Apify.getValue('FIXED-DATE')) || Date.now();
    await Apify.setValue('FIXED-DATE', fixedDate);
    const dedupSetDatasetName = `TEMPORARY-DEDUP-${fixedDate}`;
    // We never read because we don't use the parallel part
    const dedupSet = await createParallelPersistedSet(dedupSetDatasetName, { readSyncIntervalMs: 999999999 });
    log.info(`Loading deduplicating set, currently contains ${dedupSet.size()} unique keys (already deduplicated items)`);

    // We call this on every new batch of items
    const processFn = async (items, { datasetId, datasetOffset }) => {
        if (migrationState.isMigrating) {
            // Pause here and let the actor migrate
            await new Promise(() => {});
        }
        items = await preDedupTransformFn(items, { Apify, datasetId, datasetOffset, customInputData });
        // We always process the whole batch but we push only those that were not pushed
        // The order inside a single batch is stable so we can do that
        let outputItems = dedup({ items, output, fields, dedupSet });

        log.info(`[Batch-${datasetId}-${datasetOffset}]: Loaded: ${items.length}, Total unique: ${dedupSet.size()}`);

        outputItems = await postDedupTransformFn(outputItems, { Apify, datasetId, datasetOffset, customInputData });

        if (!pushState[datasetId]) {
            pushState[datasetId] = {};
        }
        if (typeof pushState[datasetId][datasetOffset] !== 'number') {
            pushState[datasetId][datasetOffset] = 0;
        }

        const pushConfig = {
            pushState,
            datasetId,
            datasetOffset,
        };

        await persistedPush({
            parallelPushes,
            outputItems,
            uploadBatchSize,
            output,
            outputDataset,
            ...pushConfig,
            outputTo,
            migrationState,
            verboseLog,
        });
    };

    const processFnNoPush = async (items, { datasetId, datasetOffset }) => {
        items = await preDedupTransformFn(items, { Apify, datasetId, datasetOffset });
        dedup({ items, output: 'nothing', fields, dedupSet });
        log.info(`[Batch-${datasetId}-${datasetOffset}]: Loaded: ${items.length}, Total unique: ${dedupSet.size()}`);
    };

    if (datasetIdsOfFilterItems) {
        // NOTE: Pepa Valek needed to skip the first time empty dataset
        const validDatasetIds = [];
        for (const datasetId of datasetIdsOfFilterItems || []) {
            const datasetInfo = await Apify.newClient().dataset(datasetId).get();
            if (datasetInfo && datasetInfo.id) {
                validDatasetIds.push(datasetId);
            } else {
                log.warning(`Dataset ${datasetId} from datasetIdsOfFilterItems does not exist, skipping...`);
            }
        }
        if (validDatasetIds.length > 0) {
            await loadDatasetItemsInParallel(
                validDatasetIds,
                {
                    fields,
                    batchSize: batchSizeLoad,
                    parallelLoads,
                    debugLog: true,
                    processFn: processFnNoPush,
                },
            );
        }
    }

    await loadDatasetItemsInParallel(
        datasetIds,
        {
            batchSize: batchSizeLoad,
            parallelLoads,
            debugLog: true,
            processFn,
            persistLoadingStateForProcesFn: true,
            // usually undefined
            fields: fieldsToLoad,
            offset,
            limit,
        },
    );

    const dedupDataset = await Apify.openDataset(dedupSetDatasetName);
    await dedupDataset.drop();
};
