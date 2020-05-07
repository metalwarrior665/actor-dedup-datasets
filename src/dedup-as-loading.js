const Apify = require('apify');
const BigMap = require('big-map-simple');
const Promise = require('bluebird');

module.exports = async ({ datasetIds, batchSizeLoad, doPush, fields, parallelLoads, outputDatasetId, uploadBatchSize, uploadSleepMs }) => {
    // Here we dedup as we go
    const dedupMap = new BigMap();
    const loadStart = Date.now();

    let totalLoaded = 0;

    for (const datasetId of datasetIds) {
        let totalLoadedPerDataset = 0;
        // We get the number of items first and then we precreate request info objects
        const { cleanItemCount } = await Apify.client.datasets.getDataset({ datasetId });
        console.log(`Dataset ${datasetId} has ${cleanItemCount} items`);
        const numberOfBatches = Math.ceil(cleanItemCount / batchSizeLoad);

        const requestInfoArr = [];
        for (let i = 0; i < numberOfBatches; i++) {
            requestInfoArr.push({
                index: i,
                offset: i * batchSizeLoad,
            });
        }

        // eslint-disable-next-line no-loop-func
        await Promise.map(requestInfoArr, async (requestInfoObj) => {
            const { items } = await Apify.client.datasets.getItems({
                datasetId,
                offset: requestInfoObj.offset,
                limit: batchSizeLoad,
                fields: doPush ? null : fields, // If we doPush, we want the full items, otherwise we take just the fields for speed
            });

            totalLoadedPerDataset += items.length;
            totalLoaded += items.length;

            console.log(
                `Items loaded from dataset ${datasetId}: ${items.length}, offset: ${requestInfoObj.offset},
        total loaded from dataset ${datasetId}: ${totalLoadedPerDataset},
        total loaded: ${totalLoaded}`,
            );


            for (const item of items) {
                const key = fields.map((field) => item[field]).join('');
                if (!dedupMap.has(key)) {
                    dedupMap.set(key, item);
                }
            }
        }, { concurrency: parallelLoads });
    }
    console.log(`Loading took ${Math.round((Date.now() - loadStart) / 1000)} seconds`);

    const valuesStart = Date.now();
    const dedupedItems = dedupMap.entries();
    console.log(`Turning Map into values took ${Math.round((Date.now() - valuesStart) / 1000)} seconds`);
    console.log(`Total loaded: ${totalLoaded}, Total unique: ${dedupedItems.length}`);

    const outputDataset = await Apify.openDataset(outputDatasetId);
    let isMigrating = false;
    Apify.events.on('migrating', () => {
        isMigrating = true;
    });

    let pushedItems = (await Apify.getValue('PUSHED')) || 0;
    const pushedKeys = (await Apify.getValue('PUSHED-KEYS')) || {};

    // Now we push from the whole BigMap
    if (doPush) {
        for (let i = pushedItems; i < dedupedItems.length; i += uploadBatchSize) {
            if (isMigrating) {
                console.log('Forever sleeping until migration');
                // Do nothing
                await new Promise(() => {});
            }
            const slice = dedupedEntries.slice(i, i + uploadBatchSize);
            const filteredSlice = slice.filter(([key, value]) => pushedKeys[key] !== true);
            const keys = filteredSlice.map(([key, value]) => key);
            const values = filteredSlice.map(([key, value]) => value);
            for (const key of keys) {
                pushedKeys[key] = true;
            }
            await outputDataset.pushData(values);
            pushedItems += values.length;
            await Apify.setValue('PUSHED', pushedItems);
            await Apify.setValue('PUSHED-KEYS', pushedKeys);
            const { itemCount } = await outputDataset.getInfo();
            console.log(`Pushed total: ${pushedItems}, In dataset (delayed): ${itemCount}`);
            await Apify.utils.sleep(uploadSleepMs);
        }
    }
}
