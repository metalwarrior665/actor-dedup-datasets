const Apify = require('apify');
const BigMap = require('big-map-simple')

Apify.main(async () => {
    // Get input of your actor
    const input = await Apify.getInput();
    console.log('My input:');
    console.dir(input);

    const {
        datasetIds,
        doPush,
        fields,
        outputDatasetId,
        uploadSleepMs = 5000,
        uploadBatchSize = 2000,
    } = input;

    if (!(Array.isArray(datasetIds) && datasetIds.length > 0)) {
        throw new Error('WRONG INPUT --- Missing datasetIds!');
    }

    if (!(Array.isArray(fields) && datasetIds.length > 0)) {
        throw new Error('WRONG INPUT --- Missing fields!');
    }

    const dedupMap = new BigMap();
    const batchSizeInit = 50000;
    let totalLoaded = 0;
    const processDataset = async (datasetId) => {
        let totalLoadedPerDataset = 0;
        let offset = 0;

        while (true) {
            const { items } = await Apify.client.datasets.getItems({
                datasetId,
                offset,
                limit: batchSizeInit,
                fields: doPush ? null : fields, // If we doPush, we want the full items, otherwise we take just the fields for speed
            });

            totalLoadedPerDataset += items.length;
            totalLoaded += items.length;

            if (items.length === 0) {
                console.log(`All items loaded from dataset: ${datasetId}`);
                break;
            }

            for (const item of items) {
                const key = fields.map((field) => item[field]).join();
                if (!dedupMap.has(key)) {
                    dedupMap.set(key, item);
                }
            }

            console.log(
                `Items loaded from dataset ${datasetId}: ${items.length},
                total loaded from dataset ${datasetId}: ${totalLoadedPerDataset},
                total loaded: ${totalLoaded},
                total unique: ${dedupMap.size}`,
            );

            offset += batchSizeInit;
        }
        const filteredItems = dedupMap.values();

        return { items: filteredItems, length: filteredItems.length };
    };

    const outputDataset = await Apify.openDataset(outputDatasetId);
    let isMigrating = false;
    Apify.events.on('migrating', () => {
        isMigrating = true;
    });

    const pushedItems = (await Apify.getValue('PUSHED')) || {};

    for (const datasetId of datasetIds) {
        const { items, length } = await processDataset(datasetId);

        if (doPush) {
            if (!(pushedItems[datasetId])) {
                pushedItems[datasetId] = 0;
            }

            for (let i = pushedItems[datasetId]; i < length; i += uploadBatchSize) {
                if (isMigrating) {
                    console.log('Forever sleeping until migration');
                    // Do nothing
                    await new Promise(() => {});
                }
                const slice = items.slice(i, i + uploadBatchSize);
                await outputDataset.pushData(slice);
                pushedItems[datasetId] += slice.length;
                await Apify.setValue('PUSHED', pushedItems);
                const { itemCount } = await outputDataset.getInfo();
                const pushedTotal = Object.values(pushedItems).reduce((acc, val) => acc + val);
                console.log(`Pushed from dataset ${datasetId}: ${pushedItems[datasetId]}, Pushed total: ${pushedTotal}, In dataset (delayed): ${itemCount}`);
                await Apify.utils.sleep(uploadSleepMs);
            }
        }
    }
});
