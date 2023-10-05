import pg from 'pg'
import chalk from 'chalk'

const colors = ['red', 'green', 'blue', 'magenta', 'cyan', 'gray']
const events = ['bobik', 'mobik', 'zigi', 'haha']

const listen = async amount => {
	console.log(chalk.green(`Starting ${amount} listeners`))
	const promises = []
	for (let i = 0; i < amount; i++) {
		const color = colors[i % colors.length]
		const conn = new pg.Client({ user: 'postgres' })
		conn.on('notification', msg => {
			console.log(chalk[color](`${i}: `), msg.payload)
		})

		promises.push(
			conn
				.connect()
				.then(() =>
					conn.query(`select pgqueue.subscribe($1, $2)`, ['test' + i, events])
				)
		)
	}
	await Promise.all(promises)
}

const emit = async delay => {
	console.log(chalk.green(`Publishing event every ${delay} ms`))
	const conn = new pg.Client({ user: 'postgres' })
	let i = 0
	conn.connect()
	setInterval(() => {
		conn.query(`select pgqueue.push($1, NULL, NULL, NULL)`, [
			events[i % events.length],
		])
		i++
	}, delay)
}

if (process.argv[2] === 'listen') {
	listen(parseInt(process.argv[3]))
} else if (process.argv[2] === 'emit') {
	emit(parseInt(process.argv[3]))
} else {
	console.log('Usage: node bobik.mjs <listen|emit> <amount|delay>')
}
