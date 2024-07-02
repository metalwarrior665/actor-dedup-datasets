const Apify = require('apify');

const dedupAfterLoadFn = require('./dedup-after-load');
const dedupAsLoadingFn = require('./dedup-as-loading');
const { validateInput } = require('./input');
const { getRealDatasetId } = require('./utils');
const { MODES } = require('./consts');
const { loadActorRunsDatasets } = require('./load-actor-runs');

const { DEDUP_AFTER_LOAD, DEDUP_AS_LOADING } = MODES;
const { log } = Apify.utils;

Apify.main(async () => {
    // Get input of your actor
    const input = await Apify.getInput();
    console.log('My input:');
    console.dir(input);

    // We limit the upload batch size because each batch must finish in the 10 seconds on migratio event
    if (input.uploadBatchSize > 1000) {
        log.warning(`Maximum uploadBatchSize can be 1000. This is to keep consistency accross migrations.`);
        input.uploadBatchSize = 1000;
    }

    if (input.mode === DEDUP_AS_LOADING && input.parallelPushes > 1) {
        log.warning(`Limiting parallel pushes to 1 because dedup as loading is already pushing in parallel by default`);
        input.parallelPushes = 1;
    }

    if (input.mode === DEDUP_AS_LOADING && input.batchSizeLoad !== input.uploadBatchSize) {
        // See NOTE in persistedPush
        log.warning(`For dedup-as-loading mode, batchSizeLoad must equal uploadBatchSize. Setting batch size to ${input.uploadBatchSize}`);
        input.batchSizeLoad = input.uploadBatchSize;
    }

    const {
        datasetIds = [],
        // If no dedup fields are supplied, we skip the deduping
        fields,
        // Also can be a name of dataset (can be created by this actor)
        outputDatasetId,
        uploadBatchSize = 1000,
        batchSizeLoad = 50000,
        parallelLoads = 10,
        parallelPushes = 5,
        offset,
        limit,
        mode = DEDUP_AFTER_LOAD,
        output = 'unique-items',
        outputTo = 'dataset',
        preDedupTransformFunction = '(items) => items',
        postDedupTransformFunction = '(items) => items',
        verboseLog = false,
        // Useful to reduce memory/traffic
        fieldsToLoad,
        // Items from these datasets will be used only to dedup against
        // Will automatically just load fields needed for dedup
        // These datasets needs to be loaded before the outputing datasets
        datasetIdsOfFilterItems,
        // This is passed to transform functions
        customInputData,

        // ID can be a name too
        actorOrTaskId, onlyRunsNewerThan, onlyRunsOlderThan,

        // Just debugging dataset duplications
        debugPlatform = false,
    } = input;

    if (debugPlatform) {
        log.setLevel(log.LEVELS.DEBUG);
    }

    validateInput({ datasetIds, actorOrTaskId, fields, output, mode, outputTo, preDedupTransformFunction, postDedupTransformFunction });
    const realOutputDatasetId = await getRealDatasetId(outputDatasetId);

    if (actorOrTaskId) {
        const loadedDatasetIds = await loadActorRunsDatasets({ actorOrTaskId, onlyRunsNewerThan, onlyRunsOlderThan });
        log.info(`Loaded ${loadedDatasetIds.length} datasets from actor runs, adding them to the total `
                + `list of ${datasetIds.length} datasets to load.`);
        for (const loadedDataset of loadedDatasetIds) {
            datasetIds.push(loadedDataset);
        }
    }

    const preDedupTransformFn = eval(preDedupTransformFunction);
    const postDedupTransformFn = eval(postDedupTransformFunction);

    const pushState = (await Apify.getValue('PUSHED')) || {};
    Apify.events.on('persistState', async () => {
        await Apify.setValue('PUSHED', pushState);
    });

    const migrationState = {
        isMigrating: false,
    };

    const migrationCallback = async () => {
        migrationState.isMigrating = true;
        log.warning(`Migrating or aborting event: Actor will persist current state and stop processing until is fully migrated.`);
    };
    Apify.events.on('migrating', migrationCallback);
    Apify.events.on('aborting', migrationCallback);

    // This is a bit dirty but we split each batch for parallel processing so it needs to grow by parallels
    const finalUploadBatchSize = uploadBatchSize * parallelPushes;

    const context = {
        datasetIds,
        // See NOTE in persistedPush
        batchSizeLoad,
        output,
        fields,
        parallelLoads,
        parallelPushes,
        outputDatasetId: realOutputDatasetId,
        uploadBatchSize: finalUploadBatchSize,
        outputTo,
        offset,
        limit,
        fieldsToLoad: Array.isArray(fieldsToLoad) && fieldsToLoad.length > 0 ? fieldsToLoad : undefined,
        datasetIdsOfFilterItems,
        preDedupTransformFn,
        postDedupTransformFn,
        pushState,
        migrationState,
        verboseLog,
        customInputData,
    };

    if (mode === DEDUP_AFTER_LOAD) {
        await dedupAfterLoadFn(context);
    } else if (mode === DEDUP_AS_LOADING) {
        await dedupAsLoadingFn(context);
    }

    await Apify.setValue('PUSHED', pushState);
});
