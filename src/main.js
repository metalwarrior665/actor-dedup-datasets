/* eslint-disable no-eval */
const Apify = require('apify');

const dedupAfterLoadFn = require('./dedup-after-load');
const dedupAsLoadingFn = require('./dedup-as-loading');
const { validateInput } = require('./input');

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
        outputTo = 'dataset',
        preDedupTransformFunction = '(items) => items',
        postDedupTransformFunction = '(items) => items',
        // Items from these datasets will be used only to dedup against
        // Will automatically just load fields needed for dedup
        // These datasets needs to be loaded before the outputing datasets
        datasetIdsOfFilterItems,
    } = input;

    validateInput({ datasetIds, fields, output, mode, outputTo, preDedupTransformFunction, postDedupTransformFunction });

    const preDedupTransformFn = eval(preDedupTransformFunction);
    const postDedupTransformFn = eval(postDedupTransformFunction);

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
        outputTo,
        datasetIdsOfFilterItems,
        preDedupTransformFn,
        postDedupTransformFn,
        pushState,
    };

    if (mode === 'dedup-after-load') {
        await dedupAfterLoadFn(context);
    } else if (mode === 'dedup-as-loading') {
        await dedupAsLoadingFn(context);
    }

    await Apify.setValue('PUSHED', pushState);
});
