/* eslint-disable no-eval */
const Apify = require('apify');

const dedupAfterLoadFn = require('./dedup-after-load');
const dedupAsLoadingFn = require('./dedup-as-loading');
const { validateInput } = require('./input');
const { betterSetInterval } = require('./utils');

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
        parallelPushes = 1,
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
    // Once we are migrating, we save the push state very often
    // to increase the chance of having the latest state
    const migrationCallback = () => {
        betterSetInterval(async () => {
            await Apify.setValue('PUSHED', pushState);
        }, 300);
    }
    Apify.events.on('migrating', migrationCallback);
    Apify.events.on('aborting', migrationCallback);

    const context = {
        datasetIds,
        batchSizeLoad,
        output,
        fields,
        parallelLoads,
        parallelPushes,
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
});
