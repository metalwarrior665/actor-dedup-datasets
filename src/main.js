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
        outputUniques = true,
        outputDuplicates = false,
        fields,
        outputDatasetId,
        uploadSleepMs = 5000,
        uploadBatchSize = 5000,
        batchSizeLoad = 50000,
        parallelLoads = 1,
        mode = 'dedup-after-load',
        output = 'unique-items',
    } = input;

    if (!(Array.isArray(datasetIds) && datasetIds.length > 0)) {
        throw new Error('WRONG INPUT --- Missing datasetIds!');
    }

    if (!(Array.isArray(fields) && datasetIds.length > 0)) {
        throw new Error('WRONG INPUT --- Missing fields!');
    }

    if (outputUniques && outputDuplicates) {
        throw new Error('WRONG INPUT --- Choose only one of outputUnique or outputDuplicates');
    }

    const context = { datasetIds, batchSizeLoad, output, fields, parallelLoads, outputDatasetId, uploadBatchSize, uploadSleepMs };

    if (mode === 'dedup-after-load') {
        await dedupAfterLoadFn(context);
    } else if (mode === 'dedup-as-loading') {
        // This path is not working yet
        await dedupAsLoadingFn(context);
    }
});
