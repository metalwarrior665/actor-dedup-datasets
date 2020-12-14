const Apify = require('apify');

const dedupAfterLoadFn = require('./dedup-after-load');
const dedupAsLoadingFn = require('./dedup-as-loading');

Apify.main(async () => {
    // Get input of your actor
    const input = await Apify.getInput();
    console.log('My input:');
    console.dir(input);

    const {
        datasetIds,
        fields,
        outputDatasetId,
        uploadSleepMs = 5000,
        uploadBatchSize = 5000,
        batchSizeLoad = 50000,
        parallelLoads = 1,
        mode = 'dedup-after-load',
        output = 'unique-items',
        // Items from these datasets will be used only to dedup against
        // Will automatically just load fields needed for dedup
        // These datasets needs to be loaded before the outputing datasets
        datasetIdsOfFilterItems,
    } = input;

    if (!(Array.isArray(datasetIds) && datasetIds.length > 0)) {
        throw new Error('WRONG INPUT --- Missing datasetIds!');
    }

    if (!(Array.isArray(fields) && datasetIds.length > 0)) {
        throw new Error('WRONG INPUT --- Missing fields!');
    }

    if (!['unique-items', 'duplicate-items', 'nothing'].includes(output)) {
        throw new Error('WRONG INPUT --- output has to be one of ["unique-items", "duplicate-items", "nothing"]');
    }

    if (!['dedup-after-load', 'dedup-as-loading'].includes(mode)) {
        throw new Error('WRONG INPUT --- output has to be one of ["dedup-after-load", "dedup-as-loading"]');
    }

    const pushState = (await Apify.getValue('PUSHED')) || {};
    Apify.events.on('persistState', async () => {
        await Apify.setValue('PUSHED', pushState);
    });

    const context = {
        datasetIds,
        batchSizeLoad,
        output,
        fields,
        parallelLoads,
        outputDatasetId,
        uploadBatchSize,
        uploadSleepMs,
        datasetIdsOfFilterItems,
        pushState,
    };

    if (mode === 'dedup-after-load') {
        await dedupAfterLoadFn(context);
    } else if (mode === 'dedup-as-loading') {
        await dedupAsLoadingFn(context);
    }
});
