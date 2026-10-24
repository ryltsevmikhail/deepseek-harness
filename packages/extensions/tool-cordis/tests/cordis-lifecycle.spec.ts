import { Context, CordisError, FiberState, type Fiber } from '@deepseek-ai/cordis'
import { describe, expect, it } from 'vitest'

/**
 * Direct regressions for the vendored Cordis ownership substrate used by
 * cordis-host-runner and cordis-client-runner and every other harness plugin.
 */

describe('Cordis consumer quiescence', () => {
  for (const teardown of ['root', 'provider', 'delete'] as const) {
    it(`keeps provider resources until already-unloading consumers stop during ${teardown} disposal`, async () => {
      const ctx = new Context()
      const started = Promise.withResolvers<undefined>()
      const finish = Promise.withResolvers<undefined>()
      const events: string[] = []
      let providerClosed = false
      const provider = (inner: Context) => {
        inner.effect(function* () {
          yield () => { providerClosed = true; events.push('provider closed') }
          yield inner.provide('quiescenceProvider', {})
        })
      }
      const consumer = Object.assign((inner: Context) => {
        inner.effect(() => async () => {
          started.resolve(undefined)
          await finish.promise
          events.push('consumer stopped')
        })
      }, { inject: ['quiescenceProvider'] })
      const providerFiber = await ctx.plugin(provider)
      const consumerFiber = await ctx.plugin(consumer)
      let closedEarly = true
      let exposedWhileStopping = true
      try {
        if (teardown === 'delete') ctx.registry.delete(consumer)
        const disposing = teardown === 'root'
          ? [ctx.fiber.dispose()]
          : [teardown === 'delete' ? consumerFiber.await() : consumerFiber.dispose(), providerFiber.dispose()]
        await started.promise
        await new Promise<void>(resolve => setImmediate(resolve))
        closedEarly = providerClosed
        exposedWhileStopping = [...(ctx.registry.get(consumer)?.fibers ?? [])].some(fiber => fiber.uid === null)
        finish.resolve(undefined)
        await Promise.all(disposing)
      } finally {
        finish.resolve(undefined)
        await ctx.fiber.dispose()
      }
      expect(closedEarly).toBe(false)
      expect(exposedWhileStopping).toBe(false)
      expect(events).toEqual(['consumer stopped', 'provider closed'])
    })
  }
  it('joins a consumer when a disposal observer synchronously withdraws its provider', async () => {
    const ctx = new Context()
    const started = Promise.withResolvers<undefined>()
    const finish = Promise.withResolvers<undefined>()
    let closed = false
    let stopProvided!: () => Promise<void>
    await ctx.plugin((inner: Context) => {
      stopProvided = inner.effect(function* () {
        yield () => { closed = true }
        yield inner.provide('reentrantProvider', {})
      })
    })
    const consumer = await ctx.plugin(Object.assign((inner: Context) => {
      inner.effect(() => async () => { started.resolve(undefined); await finish.promise })
    }, { inject: ['reentrantProvider'] }))
    let withdrawn = Promise.resolve()
    ctx.on('internal/plugin', (fiber) => {
      if (fiber === consumer.ctx.fiber && fiber.uid === null) withdrawn = Promise.resolve(stopProvided())
    })
    const disposed = consumer.dispose()
    let closedEarly = true
    try {
      await started.promise
      await new Promise<void>(resolve => setImmediate(resolve))
      closedEarly = closed
      finish.resolve(undefined)
      await Promise.all([disposed, withdrawn])
    } finally {
      finish.resolve(undefined)
      await ctx.fiber.dispose()
    }
    expect(closedEarly).toBe(false)
    expect(closed).toBe(true)
  })

  it('does not retain a stopped runtime when a status observer throws during unload', async () => {
    const ctx = new Context()
    const started = Promise.withResolvers<undefined>()
    const finish = Promise.withResolvers<undefined>()
    const provider = await ctx.plugin((inner: Context) => inner.provide('statusProvider', {}))
    const plugin = Object.assign((inner: Context) => inner.effect(() => async () => {
      started.resolve(undefined)
      await finish.promise
    }), { inject: ['statusProvider'] })
    const target = await ctx.plugin(plugin)
    ctx.on('internal/status', (fiber) => {
      if (fiber === target.ctx.fiber && fiber.state === FiberState.UNLOADING) throw new Error('broken status observer')
    })
    const disposed = target.dispose().catch((error: unknown) => error)
    try {
      await started.promise
      finish.resolve(undefined)
      const error = await disposed
      await target.await()
      expect(error).toEqual(expect.objectContaining({ message: 'broken status observer' }))
      expect(ctx.registry.has(plugin)).toBe(false)
      await provider.dispose()
    } finally {
      finish.resolve(undefined)
      await ctx.fiber.dispose()
    }
  })

  it('joins newer cleanup after an older loading transition rejects', async () => {
    const ctx = new Context()
    const starting = Promise.withResolvers<undefined>()
    const start = Promise.withResolvers<undefined>()
    const cleaning = Promise.withResolvers<undefined>()
    const finish = Promise.withResolvers<undefined>()
    let closed = false
    const provider = await ctx.plugin((inner: Context) => inner.effect(function* () {
      yield () => { closed = true }
      yield inner.provide('loadingProvider', {})
    }))
    const consumer = ctx.plugin(Object.assign(async (inner: Context) => {
      inner.effect(() => async () => { cleaning.resolve(undefined); await finish.promise })
      starting.resolve(undefined)
      await start.promise
    }, { inject: ['loadingProvider'] }))
    const loaded = Promise.resolve(consumer).catch((error: unknown) => error)
    await starting.promise
    ctx.on('internal/status', (fiber) => {
      if (fiber === consumer.ctx.fiber && fiber.state === FiberState.UNLOADING) throw new Error('broken loading observer')
    })
    const disposed = consumer.dispose().catch((error: unknown) => error)
    const withdrawing = provider.dispose()
    let closedEarly = true
    try {
      start.resolve(undefined)
      await cleaning.promise
      await new Promise<void>(resolve => setImmediate(resolve))
      closedEarly = closed
      finish.resolve(undefined)
      const error = await disposed
      await consumer.ctx.fiber.await()
      await loaded
      await withdrawing
      expect(error).toEqual(expect.objectContaining({ message: 'broken loading observer' }))
    } finally {
      start.resolve(undefined)
      finish.resolve(undefined)
      await ctx.fiber.dispose()
    }
    expect(closedEarly).toBe(false)
  })

  it('preserves a new runtime registered by an old runtime disposal observer', async () => {
    const ctx = new Context()
    const finish = Promise.withResolvers<undefined>()
    const plugin = (inner: Context) => inner.effect(() => async () => { await finish.promise })
    const old = await ctx.plugin(plugin)
    let replacement: ReturnType<Context['plugin']> | undefined
    ctx.on('internal/plugin', (fiber) => {
      if (fiber === old.ctx.fiber && fiber.uid === null) {
        ctx.registry.delete(plugin)
        replacement = ctx.plugin(plugin)
      }
    })
    const stopped = old.dispose()
    try {
      if (replacement === undefined) throw new Error('replacement was not registered')
      const next = await replacement
      finish.resolve(undefined)
      await stopped
      expect(ctx.registry.get(plugin)).toBe(next.runtime)
      expect(next.uid).not.toBeNull()
    } finally {
      finish.resolve(undefined)
      await ctx.fiber.dispose()
    }
  })

  it('does not reactivate a stopping consumer when its provider is replaced', async () => {
    const ctx = new Context()
    const started = Promise.withResolvers<undefined>()
    const finish = Promise.withResolvers<undefined>()
    let starts = 0
    let stops = 0
    const provider = await ctx.plugin((inner: Context) => inner.provide('replacementProvider', {}))
    const consumer = await ctx.plugin(Object.assign((inner: Context) => {
      starts += 1
      inner.effect(() => async () => { started.resolve(undefined); await finish.promise; stops += 1 })
    }, { inject: ['replacementProvider'] }))
    try {
      const stopped = consumer.dispose()
      await started.promise
      const withdrawn = provider.dispose()
      await new Promise<void>(resolve => setImmediate(resolve))
      await ctx.plugin((inner: Context) => inner.provide('replacementProvider', {}))
      finish.resolve(undefined)
      await Promise.all([stopped, withdrawn])
      expect(starts).toBe(1)
      expect(stops).toBe(1)
    } finally {
      finish.resolve(undefined)
      await ctx.fiber.dispose()
    }
  })
})

describe('Cordis effect ownership', () => {
  it('makes an effect visible to a reentrant owner restart and awaits setup plus cleanup', async () => {
    const ctx = new Context()
    const setupGate = Promise.withResolvers<undefined>()
    const cleanupGate = Promise.withResolvers<undefined>()
    const cleanupStarted = Promise.withResolvers<undefined>()
    let restarted!: Promise<void>
    let setupFinished = false
    let cleanupFinished = false

    ctx.effect(async () => {
      restarted = ctx.fiber.restart()
      await setupGate.promise
      setupFinished = true
      return async () => {
        cleanupStarted.resolve(undefined)
        await cleanupGate.promise
        cleanupFinished = true
      }
    }, 'reentrant-restart')

    let settled = false
    void restarted.then(() => { settled = true })
    await Promise.resolve()
    expect(settled).toBe(false)

    setupGate.resolve(undefined)
    await cleanupStarted.promise
    expect(setupFinished).toBe(true)
    await Promise.resolve()
    expect(settled).toBe(false)

    cleanupGate.resolve(undefined)
    await restarted
    expect(cleanupFinished).toBe(true)
    expect(ctx.fiber.getEffects()).toEqual([])
  })

  it('rolls back collected cleanup and its owner-list entry when setup throws synchronously', () => {
    const ctx = new Context()
    let cleanups = 0

    expect(() => ctx.effect(function* () {
      yield () => { cleanups += 1 }
      throw new Error('setup failed')
    }, 'throwing-setup')).toThrow('setup failed')

    expect(cleanups).toBe(1)
    expect(ctx.fiber.getEffects()).toEqual([])
  })

  it('makes a reentrant owner restart await asynchronous rollback after synchronous setup failure', async () => {
    const ctx = new Context()
    const cleanupGate = Promise.withResolvers<undefined>()
    const cleanupStarted = Promise.withResolvers<undefined>()
    let restarted!: Promise<void>

    expect(() => ctx.effect(function* () {
      yield async () => {
        cleanupStarted.resolve(undefined)
        await cleanupGate.promise
      }
      restarted = ctx.fiber.restart()
      throw new Error('setup failed after restart')
    }, 'reentrant-throw')).toThrow('setup failed after restart')

    await cleanupStarted.promise
    let settled = false
    void restarted.then(() => { settled = true })
    await Promise.resolve()
    expect(settled).toBe(false)

    cleanupGate.resolve(undefined)
    await restarted
    expect(ctx.fiber.getEffects()).toEqual([])
  })

  it('keeps ordinary teardown synchronous and the public disposer single-shot', () => {
    const ctx = new Context()
    let cleanups = 0
    const dispose = ctx.effect(() => () => { cleanups += 1 }, 'sync-effect')

    expect(dispose()).toBeUndefined()
    expect(cleanups).toBe(1)
    expect(dispose()).toBeUndefined()
    expect(cleanups).toBe(1)
    expect(ctx.fiber.getEffects()).toEqual([])
  })

  it('rejects cleanup-time registration while a restart is unloading', async () => {
    const ctx = new Context()
    let registrationError: unknown

    ctx.effect(() => () => {
      try {
        ctx.effect(() => () => {}, 'too-late')
      } catch (error) {
        registrationError = error
      }
    }, 'restart-cleanup')

    await ctx.fiber.restart()
    expect(registrationError).toBeInstanceOf(CordisError)
    expect((registrationError as CordisError).code).toBe('INACTIVE_EFFECT')
    expect(ctx.fiber.state).toBe(FiberState.ACTIVE)
    expect(ctx.fiber.getEffects()).toEqual([])
  })

  it('keeps effect registration legal while child fibers are PENDING and LOADING', async () => {
    const ctx = new Context()
    let pendingCleanup = false
    let loadingCleanup = false

    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name !== 'state-probe' || fiber.uid === null) return
      expect(fiber.state).toBe(FiberState.PENDING)
      fiber.ctx.effect(() => () => { pendingCleanup = true }, 'pending-effect')
    })

    const fiber = await ctx.plugin({
      name: 'state-probe',
      apply(inner) {
        expect(inner.fiber.state).toBe(FiberState.LOADING)
        inner.effect(() => () => { loadingCleanup = true }, 'loading-effect')
      },
    })
    await fiber.dispose()

    expect(pendingCleanup).toBe(true)
    expect(loadingCleanup).toBe(true)
  })

  it('resolves dependencies that internal/plugin adds before child activation', async () => {
    const ctx = new Context()
    ctx.provide('late-inject', {})
    let applyCalls = 0

    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name !== 'loader-shaped' || fiber.uid === null) return
      fiber.inject['late-inject'] = {}
    })

    const fiber = await ctx.plugin({
      name: 'loader-shaped',
      apply() {
        applyCalls += 1
      },
    })

    expect(applyCalls).toBe(1)
    expect(fiber.state).toBe(FiberState.ACTIVE)
  })
})

describe('Cordis child publication ownership', () => {
  it('rolls back parent and runtime ownership when internal/plugin publication throws', () => {
    const ctx = new Context()
    const plugin = { name: 'publication-failure', apply() {} }
    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name === plugin.name) throw new Error('publication failed')
    })

    expect(() => ctx.plugin(plugin)).toThrow('publication failed')
    expect(ctx.registry.has(plugin)).toBe(false)
  })

  it('contains teardown notification failures so ownership cleanup and peers complete', async () => {
    const ctx = new Context()
    const errors: unknown[] = []
    ctx.logger.error = ((error: unknown) => { errors.push(error) }) as typeof ctx.logger.error
    const observed: string[] = []
    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name === 'contained-teardown' && fiber.uid === null) {
        throw new Error('broken teardown observer')
      }
    })
    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name === 'contained-teardown' && fiber.uid === null) observed.push('disposed')
    })
    const child = await ctx.plugin({ name: 'contained-teardown', apply() {} })

    await expect(child.dispose()).resolves.toBeUndefined()
    expect(observed).toEqual(['disposed'])
    expect(errors).toHaveLength(1)
    expect(errors[0]).toEqual(expect.objectContaining({ message: 'broken teardown observer' }))
    expect(child.uid).toBeNull()
  })

  it('makes a LOADING parent join child cleanup started before its unload snapshot', async () => {
    const ctx = new Context()
    const cleanupGate = Promise.withResolvers<undefined>()
    const cleanupStarted = Promise.withResolvers<undefined>()
    let ownerFiber!: Fiber
    let ownerDisposal!: Promise<void>
    let childDisposal!: Promise<void>
    let childFiber!: Fiber

    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name !== 'loading-child' || fiber.uid === null) return
      childFiber = fiber
      fiber.ctx.effect(() => async () => {
        cleanupStarted.resolve(undefined)
        await cleanupGate.promise
      }, 'loading-child-cleanup')
      ownerDisposal = ownerFiber.dispose()
      childDisposal = Promise.resolve(fiber.dispose())
    })

    const ownerMount = ctx.plugin({
      name: 'loading-owner',
      apply(inner) {
        ownerFiber = inner.fiber
        inner.plugin({ name: 'loading-child', apply() {} })
      },
    })

    await cleanupStarted.promise
    let ownerSettled = false
    void ownerDisposal.then(() => { ownerSettled = true })
    await Promise.resolve()
    expect(ownerSettled).toBe(false)

    cleanupGate.resolve(undefined)
    await Promise.all([ownerDisposal, childDisposal, ownerMount])
    expect(childFiber.uid).toBeNull()
    expect(ownerFiber.uid).toBeNull()
  })

  it('lets parent disposal during internal/plugin await the unpublished child to quiescence', async () => {
    const ctx = new Context()
    let ownerCtx!: Context
    const owner = await ctx.plugin({
      name: 'owner',
      apply(inner) {
        ownerCtx = inner
      },
    })

    const cleanupGate = Promise.withResolvers<undefined>()
    const cleanupStarted = Promise.withResolvers<undefined>()
    let cleanupFinished = false
    let childApplyCalls = 0
    let parentDisposal!: Promise<void>

    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name !== 'child' || fiber.uid === null) return
      expect(fiber.state).toBe(FiberState.PENDING)
      fiber.ctx.effect(() => async () => {
        cleanupStarted.resolve(undefined)
        await cleanupGate.promise
        cleanupFinished = true
      }, 'pending-child-cleanup')
    })
    ctx.on('internal/plugin', (fiber) => {
      if (fiber.name !== 'child' || fiber.uid === null) return
      parentDisposal = owner.dispose()
    })

    const child = ownerCtx.plugin({
      name: 'child',
      apply() {
        childApplyCalls += 1
      },
    })

    await cleanupStarted.promise
    let settled = false
    void parentDisposal.then(() => { settled = true })
    await Promise.resolve()
    expect(settled).toBe(false)

    cleanupGate.resolve(undefined)
    await parentDisposal
    expect(cleanupFinished).toBe(true)
    expect(childApplyCalls).toBe(0)
    expect(child.uid).toBeNull()
    expect(child.state).toBe(FiberState.DISPOSED)
  })
})
