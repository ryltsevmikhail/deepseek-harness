/** Private consumer disposal completion, independent of active plugin registrations. */
import type { Context } from './context.ts'
import type { Fiber } from './fiber.ts'

// Disposal ownership is independent of the active plugin registry used by HMR.
const unloading = new WeakMap<Context, Map<Fiber, PromiseWithResolvers<undefined>>>()

/**
 * Retain a consumer until its acquired work finishes.
 * @param fiber - The consumer being disposed.
 * @returns release of its private disposal registration.
 */
export function trackUnloading(fiber: Fiber): () => void {
  const root = fiber.ctx.root
  let fibers = unloading.get(root)
  if (fibers === undefined) unloading.set(root, fibers = new Map())
  const completed = Promise.withResolvers<undefined>()
  fibers.set(fiber, completed)
  return () => {
    fibers.delete(fiber)
    if (fibers.size === 0) unloading.delete(root)
    completed.resolve(undefined)
  }
}

/**
 * Find consumers that have left the active registry but still own work.
 * @param ctx - Context selecting the independent root.
 * @returns the root's currently unloading consumers.
 */
export function unloadingFibers(ctx: Context): Iterable<Fiber> {
  return unloading.get(ctx.root)?.keys() ?? []
}

/**
 * Join disposal from its entry, including before transition observers return.
 * @param fiber - The consumer whose disposal may already have started.
 * @returns its disposal completion, or `undefined` for an active consumer.
 */
export function unloadingCompletion(fiber: Fiber): Promise<undefined> | undefined {
  return unloading.get(fiber.ctx.root)?.get(fiber)?.promise
}
