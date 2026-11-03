import { useEffect, useRef } from 'react';

type PollOptions = {
  delay: () => number;
  visible?: () => boolean;
  onError?: (error: unknown) => void;
  setTimer?: (callback: () => void, delay: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
};

export function createPoller(task: (signal:AbortSignal) => Promise<unknown>, options: PollOptions) {
  const setTimer: NonNullable<PollOptions['setTimer']> = options.setTimer || ((callback, delay) => setTimeout(callback, delay));
  const clearTimer: NonNullable<PollOptions['clearTimer']> = options.clearTimer || (timer => clearTimeout(timer));
  let stopped = false;
  let running = false;
  let requested = false;
  let controller:AbortController|undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  function cancelTimer() {if (timer !== undefined) {clearTimer(timer);timer = undefined;}}
  async function refresh() {
    cancelTimer();
    if (stopped || (options.visible && !options.visible())) return;
    if (running) {requested = true;return;}
    running = true;
    controller=new AbortController();
    const signal=controller.signal;
    try {await task(signal);} catch (error) {if (!stopped&&!signal.aborted) options.onError?.(error);}
    finally {
      running = false;
      if (!stopped&&(!options.visible||options.visible())) {
        const delay = requested ? 0 : options.delay();
        requested = false;
        timer = setTimer(() => void refresh(), delay);
      }
    }
  }
  return {refresh,suspend(){requested=false;cancelTimer();controller?.abort();},
    stop() {stopped = true;cancelTimer();controller?.abort();}};
}

export function usePolling(task: (signal?:AbortSignal) => Promise<unknown>, intervalMs: number | null,
    options: {immediate?: boolean; onError?: (error: unknown) => void} = {}) {
  const taskRef = useRef(task);
  const errorRef = useRef(options.onError);
  taskRef.current = task;
  errorRef.current = options.onError;
  const immediate = options.immediate !== false;
  useEffect(() => {
    if (intervalMs === null) return;
    const poller = createPoller(signal => taskRef.current(signal), {
      delay: () => intervalMs,
      visible: () => !document.hidden,
      onError: error => errorRef.current?.(error),
    });
    let initial: ReturnType<typeof setTimeout> | undefined;
    const visible = () => {
      if(initial!==undefined){clearTimeout(initial);initial=undefined;}
      if (!document.hidden) void poller.refresh();else poller.suspend();
    };
    if (immediate) void poller.refresh();
    else initial = setTimeout(() => void poller.refresh(), intervalMs);
    document.addEventListener('visibilitychange', visible);
    return () => {if (initial !== undefined) clearTimeout(initial);poller.stop();document.removeEventListener('visibilitychange', visible);};
  }, [intervalMs, immediate]);
}
