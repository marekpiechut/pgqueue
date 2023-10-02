import * as pgqueue from '@pgqueue/pgqueue'
import { id } from '@pgqueue/core'
import pg from 'pg'
import { WebSocketServer } from 'ws'

const PORTS = [8080, 8081]

type UUID = string
type Hello = {
	type: 'HELLO'
	name: string
	color: string
}
type Message = {
	type: 'MESSAGE'
	from: UUID
	text: string
}
type Rename = {
	type: 'RENAME'
	name: string
	color: string
}
type Bye = {
	type: 'BYE'
	id: UUID
}
type Connected = {
	type: 'CONNECTED'
	id: UUID
}
type ClientMessage = Hello | Message | Rename | Bye | Connected
type ServerMessage = ClientMessage & {
	id: UUID
	users?: Record<UUID, { name: string; color: string }>
}

const start = async (port: number): Promise<void> => {
	const users = {}

	const { broadcaster } = await pgqueue.startWithPool(
		new pg.Pool({
			user: 'postgres',
			host: 'localhost',
		})
	)
	const ws = new WebSocketServer({
		port,
	})

	const notifyUsers = (msg: ServerMessage): void => {
		ws.clients.forEach(socket => {
			socket.send(JSON.stringify(msg))
		})
	}

	broadcaster.on('chat', async msg => {
		const data = msg.payload as ClientMessage & { id: UUID }
		const id = data.id
		if (data.type === 'HELLO') {
			const user = users[id] || { id }
			user.name = data.name
			user.color = data.color
			users[id] = user
			notifyUsers({ ...data, users })
		} else if (data.type === 'MESSAGE') {
			notifyUsers(data)
		} else if (data.type === 'RENAME') {
			const user = users[id] || { id }
			user.name = data.name
			user.color = data.color
			users[id] = user
			notifyUsers({ ...data, users })
		} else if (data.type === 'BYE') {
			delete users[id]
			notifyUsers({ ...data, users })
		}
	})

	ws.on('error', e => console.error(port, e))
	ws.on('connection', socket => {
		const id = id.uuidgen()
		console.log(port, 'Client connected', id)

		users[id] = { id }

		socket.send(JSON.stringify({ type: 'CONNECTED', users, id }))

		socket.on('message', message => {
			const data = JSON.parse(message.toString())
			broadcaster.emit('chat', { ...data, id })
		})
		socket.on('close', () => {
			console.log(port, 'Client disconnected', id)
			broadcaster.emit('chat', { type: 'BYE', id })
		})
	})

	console.log(port, `Socket server running on port ${port}`)
}

for (const port of PORTS) {
	start(port).catch(console.error)
}
