import { useEffect, useRef } from 'react';

type PollOptions = {
  delay: () => number;
  visible?: () => boolean;
  onError?: (error: unknown) => void;
  setTimer?: (callback: () => void, delay: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
};

export function createPoller(task: () => Promise<unknown>, options: PollOptions) {
  const setTimer: NonNullable<PollOptions['setTimer']> = options.setTimer || ((callback, delay) => setTimeout(callback, delay));
  const clearTimer: NonNullable<PollOptions['clearTimer']> = options.clearTimer || (timer => clearTimeout(timer));
  let stopped = false;
  let running = false;
  let requested = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  function cancelTimer() {if (timer !== undefined) {clearTimer(timer);timer = undefined;}}
  async function refresh() {
    cancelTimer();
    if (stopped || (options.visible && !options.visible())) return;
    if (running) {requested = true;return;}
    running = true;
    try {await task();} catch (error) {if (!stopped) options.onError?.(error);}
    finally {
      running = false;
      if (!stopped) {
        const delay = requested ? 0 : options.delay();
        requested = false;
        timer = setTimer(() => void refresh(), delay);
      }
    }
  }
  return {refresh, stop() {stopped = true;cancelTimer();}};
}

export function usePolling(task: () => Promise<unknown>, intervalMs: number | null,
    options: {immediate?: boolean; onError?: (error: unknown) => void} = {}) {
  const taskRef = useRef(task);
  const errorRef = useRef(options.onError);
  taskRef.current = task;
  errorRef.current = options.onError;
  const immediate = options.immediate !== false;
  useEffect(() => {
    if (intervalMs === null) return;
    const poller = createPoller(() => taskRef.current(), {
      delay: () => intervalMs,
      visible: () => !document.hidden,
      onError: error => errorRef.current?.(error),
    });
    const visible = () => {if (!document.hidden) void poller.refresh();};
    let initial: ReturnType<typeof setTimeout> | undefined;
    if (immediate) void poller.refresh();
    else initial = setTimeout(() => void poller.refresh(), intervalMs);
    document.addEventListener('visibilitychange', visible);
    return () => {if (initial !== undefined) clearTimeout(initial);poller.stop();document.removeEventListener('visibilitychange', visible);};
  }, [intervalMs, immediate]);
}
