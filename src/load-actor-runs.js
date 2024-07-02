const Apify = require('apify');

const { log } = Apify.utils;

module.exports.loadActorRunsDatasets = async ({ actorOrTaskId, onlyRunsNewerThan, onlyRunsOlderThan }) => {
    let onlyRunsNewerThanDate;

    if (onlyRunsNewerThan) {
        onlyRunsNewerThanDate = new Date(onlyRunsNewerThan);
        if (Number.isNaN(onlyRunsNewerThanDate.getTime())) {
            throw new Error('Invalid date format for onlyRunsNewerThan, use YYYY-MM-DD or with time YYYY-MM-DDTHH:mm:ss');
        }
    }

    let onlyRunsOlderThanDate;

    if (onlyRunsOlderThan) {
        onlyRunsOlderThanDate = new Date(onlyRunsOlderThan);
        if (Number.isNaN(onlyRunsOlderThanDate.getTime())) {
            throw new Error('Invalid date format for onlyRunsOlderThan, use YYYY-MM-DD or with time YYYY-MM-DDTHH:mm:ss');
        }
    }

    const client = Apify.newClient();

    // We check if the ID is a task, if not we assume it is an Actor
    const isTask = !!(await client.task(actorOrTaskId).get());

    const runsClient = client[isTask ? 'task' : 'actor'](actorOrTaskId).runs();

    const state = await Apify.getValue('STATE') || { lastProcessedOffset: 0, lastProcessedRunId: null, dateAggregations: {} };

    Apify.events.on('persistState', async () => {
        await Apify.setValue('STATE', state);
    });

    let isMigrating = false;
    let foundLastProcessedRun = false;

    Apify.events.on('migrating', async () => {
        isMigrating = true;
        log.warning('Migrating event: Actor will persist current state and stop processing until is fully migrated.');
    })

    const runsToProcess = [];

    const LIMIT = 1000;
    let offset = state.lastProcessedOffset;
    for (; ;) {
        const runs = await runsClient.list({ desc: true, limit: 1000, offset }).then((res) => res.items);

        log.info(`Loaded ${runs.length} runs (offset from now: ${offset}), newest: ${runs[0]?.startedAt}, `
            + `oldest: ${runs[runs.length - 1]?.startedAt} processing them now`);

        let stopLoop = false;

        for (const run of runs) {
            if (isMigrating) {
                log.warning('Actor is migrating, pausing all processing and storing last state to continue where we left of');
                state.lastProcessedRunId = run.id;
                await new Promise(res => setTimeout(res, 999999));
            }
    
            // If we load after migration, we need to find run we already processed
            if (state.lastProcessedRunId && !foundLastProcessedRun) {
                const isLastProcessed = state.lastProcessedRunId === run.id;
                if (isLastProcessed) {
                    foundLastProcessedRun = true;
                    state.lastProcessedRunId = null;
                } else {
                    log.warning(`Skipping run we already processed before migration ${run.id}`);
                    continue;
                }
            }
    
            if (onlyRunsOlderThanDate && run.startedAt > onlyRunsOlderThanDate) {
                continue;
            }
            if (onlyRunsNewerThanDate && run.startedAt < onlyRunsNewerThanDate) {
                // We are going from present to past so at this point we can exit
                stopLoop = true;
                break;
            }

            runsToProcess.push(run);
        }

        state.lastProcessedOffset = offset;

        if (stopLoop) {
            log.warning(`Reached onlyRunsNewerThanDate ${onlyRunsNewerThanDate}, stopping loading runs`);
            break;
        }

        if (runs.length < LIMIT) {
            log.warning('No more runs to process, stopping loading runs');
            break;
        }

        offset += LIMIT;
    }

    await Apify.setValue('STATE', runsToProcess);

    const datasetIds = runsToProcess.map(run => run.defaultDatasetId);
    return datasetIds;
}