import { DEFAULT_SCHEMA } from './core/index.js'

import evolutions, { Evolutions } from './evolutions.js'
import processors, { Processors } from './processors.js'
import queues, { Queue } from './queues.js'

export { DEFAULT_SCHEMA, evolutions, processors, queues }
export type { Processors, Queue, Evolutions }

export default {
	DEFAULT_SCHEMA,
	queues,
	evolutions,
	processors,
}
