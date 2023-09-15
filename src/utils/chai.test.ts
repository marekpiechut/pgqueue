import chai, { assert } from 'chai'
import chaiAsPromised from 'chai-as-promised'
import chaiString from 'chai-string'

chai.use(chaiAsPromised)
chai.use(chaiString)

export { chai, assert }
export default chai
