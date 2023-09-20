import sinon from 'sinon'
import chai, { assert } from 'chai'
import chaiAsPromised from 'chai-as-promised'
import chaiString from 'chai-string'
import { logger } from '@pgqueue/core'

chai.use(chaiAsPromised)
chai.use(chaiString)

export { chai, assert }
export * as async from './async'

export const mochaHooks = {
	beforeEach(): void {
		logger.setLevel('none')
	},
	afterEach(): void {
		sinon.restore()
	},
}
