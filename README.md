# PGQueue - multi-tenant Queue and Schedule library for NodeJS/Bun running on PostgreSQL

A multi-tenant, scalable queue/schedule solution for Node/Bun based on PostgreSQL.

> [!IMPORTANT]
> This library is currently in Alpha quality stage. We're using it in our
> serverless queues product: [LambdaQueue](https://lambdaqueue.com), but it
> was not tested in other uses.

## Features

This library provides a general queueing and scheduling mechanisms for NodeJS/Bun. It has support for:

- Multiple queues and schedules
- Cron/simple recurring schedules (separate from jobs)
- Retries with exponential backoff
- Arbitrary item payloads and results
- Multi-tenancy - you can have tasks from multiple users and they cannot see other's tasks
- Queues/schedules statistics API
- Automatic schema migrations
- Scalability - you can have multiple runners and schedulers and they will not collide
- Job History - History of executed jobs is saved in partitioned tables for performance
- No assumption about how many tenants or queues you will create
- User provided unique key, so you can replace and delete jobs without storing job_id`

## Planned

- Long running jobs with timeout extended with API
- Task dependencies for flow/sagas
- Automatic maintenance of job history partitions

## Multi-tenancy/Multi worker

This library is built around multi-tenancy and multiple worker types support.
It also assumes that you'll be running multiple instances doing parts
of the work. It makes things more complicated than they could be if what you need is a simple
work queue. It might be worth checking out [PGBoss](https://github.com/timgit/pg-boss)
if you don't need all that.

## Installation

```bash

npm install @dayone-labs/pgqueue

```

## Example

### Before you start

We need at least 2 separate Postgres users, but we recommend 3:

- Admin - used only to create and update schema (you can just use `lqworker` for that, but separate user is more secure)
- Worker - has access to all queue items
- User - this user has row level security enabled. It has to run in tenant context or won't see anything.

```sql
create user lqadmin with password 'lqadmin';
create user lquser with password 'lquser';
create user lqworker with password 'lqworker';

-- If we create roles manually we don't need to mess with that later
create role QUEUE_WORKER;
create role QUEUE_USER;

grant QUEUE_WORKER to lqworker;
grant QUEUE_USER to lquser;
```

### Service that will be scheduling/pushing tasks

```typescript
import pgqueue from '@dayone-labs/pgqueue'

//Run once with user that is the db owner
await pgqueue.initialize('postgres://lqadmin:lqadmin@localhost/lq')

// Global client, can be passed to all code that needs to push to queues or create schedules
// Quickstart call is a great starting point for services that need to push/schedule stuff
// but you may also use every service separately (Queues and Schedules)
// check `@dayone-labs/pgqueue/schedules` and `@dayone-labs/pgqueue/queues` imports
const client = await pgqueue.quickstart('postgres://lquser:lquser@localhost/lq')

//Push some work to queue
const dummyPayload = Buffer.from('this-is-data', 'utf8');
client.queues.withTenant('super-rich-user').push({
  queue: 'most-important-tasks',
  delay: 20_000,
  payload: dummyPayload,
  payloadType: 'text/plain'
  type: 'to separate workers, ex: HTTP, FTP, RUN, ARCHIVE',
  target: {
    some: 'whatever your worker needs to handle this, ex:',
    url: 'https://my-service.com',
    method: 'PUT'
  }
})
```

### Workers

Workers are separated, so you can scale them as you need.

```typescript
import pgqueue from '@dayone-labs/pgqueue'
import * as queues from '@dayone-labs/pgqueue/queues'
//Run once with user that is the db owner
await pgqueue.initialize('postgres://lqadmin:lqadmin@localhost/lq') 

const worker = queues.Worker.create(
  pgUrl,
  {nodeId: },
  async item => {
    await doSomeProcessing(item)
    //You can return a `WorkResult` object here and it will be persisted in history
  } 
)
await worker.start()
```

### Job Schedulers/Schedule Runners/Maintenance Runners

Schedulers push work from queues to workers, making sure that we pull work from
different tenants (so 1 tenant cannot starve others). They also maintain schedules,
so jobs are pushed to queues when needed. From time to time, they also run periodic
maintenance jobs like creation of new history partitions, etc.

I tend to keep them all in one service, as they are usually not overloaded with work,
but nothing stops you from running separate nodes with each one of them.

You need at least one of these, but I would recommend to run at least 2, so your service is uninterrupted.

```typescript
import pgqueue from '@dayone-labs/pgqueue'
import { Maintenance } from '@dayone-labs/pgqueue/maintenance'
import { Scheduler } from '@dayone-labs/pgqueue/queues'
import { ScheduleRunner } from '@dayone-labs/pgqueue/schedules'

await pgqueue.initialize(`postgres://lqadmin:lqadmin@localhost/lq`)

//Of course you can run each in it's own service

const workScheduler = Scheduler.create(`postgres://lqworker:lqworker@localhost/lq`)
workScheduler.start()

const scheduleRunner = ScheduleRunner.create(`postgres://lqworker:lqworker@localhost/lq`)
scheduleRunner.start()

const maintenance = Maintenance.create(`postgres://lqadmin:lqadmin@localhost/lq`)
maintenance.start()
```

## Work in progress

There's a lot of documentation missing here. There's a lot of options
you can pass to individual components and services. You can have worker
metadata stored, configure batch sizes and polling interval, etc.

Play with code if you like. Skip it if you don't. I'll try to add
documentation in my free time, but this library is a fast moving target,
it will change a lot while I'm building my product [LambdaQueue](https://lambdaqueue.com).
New features will be added and old ones refined.


## Contributing

Please feel free to submit improvement ideas or bug reports.
Unfortunately it's closed for **code contributions** at the moment.
Managing pull requests is just a bit too much for me now.

